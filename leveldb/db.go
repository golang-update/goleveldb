// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

// DB represent a database session.
type DB struct {
	// Need 64-bit alignment.
	seq, fseq uint64

	s *session

	cch     chan cSignal       // compaction worker signal
	creq    chan *cReq         // compaction request
	wlock   chan struct{}      // writer mutex
	wqueue  chan *Batch        // writer queue
	wack    chan error         // writer ack
	jch     chan *Batch        // journal writer chan
	jack    chan error         // journal writer ack
	ewg     sync.WaitGroup     // exit WaitGroup
	cstats  [kNumLevels]cStats // Compaction stats
	closeCb func() error

	mem      unsafe.Pointer
	journal  *journalWriter
	fjournal *journalWriter
	snaps    *snaps
	closed   uint32
	err      unsafe.Pointer
}

func openDB(s *session) (*DB, error) {
	db := &DB{
		s:      s,
		cch:    make(chan cSignal),
		creq:   make(chan *cReq),
		wlock:  make(chan struct{}, 1),
		wqueue: make(chan *Batch),
		wack:   make(chan error),
		jch:    make(chan *Batch),
		jack:   make(chan error),
		seq:    s.stSeq,
		snaps:  newSnaps(),
	}

	if err := db.recoverJournal(); err != nil {
		return nil, err
	}

	// remove any obsolete files
	db.cleanFiles()

	db.ewg.Add(2)
	go db.compaction()
	go db.writeJournal()
	// wait for compaction goroutine
	db.cch <- cWait

	runtime.SetFinalizer(db, (*DB).Close)
	return db, nil
}

// Open open or create database from given storage.
func Open(p storage.Storage, o *opt.Options) (*DB, error) {
	s, err := openSession(p, o)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	err = s.recover()
	if os.IsNotExist(err) && s.o.HasFlag(opt.OFCreateIfMissing) {
		err = s.create()
	} else if err == nil && s.o.HasFlag(opt.OFErrorIfExist) {
		err = os.ErrExist
	}
	if err != nil {
		return nil, err
	}

	return openDB(s)
}

// OpenFile open or create database from given file.
//
// This is alias of:
//	stor, err := storage.OpenFile("path/to/db")
//	...
//	db, err := Open(stor, &opt.Options{})
//	...
func OpenFile(path string, o *opt.Options) (*DB, error) {
	stor, err := storage.OpenFile(path)
	if err != nil {
		return nil, err
	}
	db, err := Open(stor, o)
	db.closeCb = func() error {
		return stor.Close()
	}
	return db, err
}

// Recover recover database with missing or corrupted manifest file. It will
// ignore any manifest files, valid or not.
func Recover(p storage.Storage, o *opt.Options) (*DB, error) {
	s, err := openSession(p, o)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			s.close()
		}
	}()

	// get all files
	ff := files(s.getFiles(storage.TypeAll))
	ff.sort()

	s.printf("Recover: started, files=%d", len(ff))

	rec := new(sessionRecord)

	// recover tables
	ro := &opt.ReadOptions{}
	var nt *tFile
	for _, f := range ff {
		if f.Type() != storage.TypeTable {
			continue
		}

		var size uint64
		size, err = f.Size()
		if err != nil {
			return nil, err
		}

		t := newTFile(f, size, nil, nil)
		iter := s.tops.newIterator(t, ro)
		// min ikey
		if iter.First() {
			t.min = iter.Key()
		} else if err := iter.Error(); err != nil {
			return nil, err
		} else {
			continue
		}
		// max ikey
		if iter.Last() {
			t.max = iter.Key()
		} else if err := iter.Error(); err != nil {
			return nil, err
		} else {
			continue
		}

		// add table to level 0
		rec.addTableFile(0, t)

		nt = t
	}

	// extract largest seq number from newest table
	if nt != nil {
		var lseq uint64
		iter := s.tops.newIterator(nt, ro)
		for iter.Next() {
			seq, _, ok := iKey(iter.Key()).parseNum()
			if !ok {
				continue
			}
			if seq > lseq {
				lseq = seq
			}
		}
		rec.setSeq(lseq)
	}

	// set file num based on largest one
	s.stFileNum = ff[len(ff)-1].Num() + 1

	// create brand new manifest
	if err = s.create(); err != nil {
		return nil, err
	}
	// commit record
	if err = s.commit(rec); err != nil {
		return nil, err
	}

	return openDB(s)
}

func (d *DB) recoverJournal() error {
	s := d.s
	icmp := s.cmp

	s.printf("JournalRecovery: started, min=%d", s.stJournalNum)

	var mem *memdb.DB
	batch := new(Batch)
	cm := newCMem(s)

	journals := files(s.getFiles(storage.TypeJournal))
	journals.sort()
	rJournals := make([]storage.File, 0, len(journals))
	for _, journal := range journals {
		if journal.Num() >= s.stJournalNum || journal.Num() == s.stPrevJournalNum {
			s.markFileNum(journal.Num())
			rJournals = append(rJournals, journal)
		}
	}

	var fr *journalReader
	for _, journal := range rJournals {
		s.printf("JournalRecovery: recovering, num=%d", journal.Num())

		r, err := newJournalReader(journal, true, s.journalDropFunc("journal", journal.Num()))
		if err != nil {
			return err
		}

		if mem != nil {
			if mem.Len() > 0 {
				if err = cm.flush(mem, 0); err != nil {
					return err
				}
			}

			if err = cm.commit(r.file.Num(), d.seq); err != nil {
				return err
			}

			cm.reset()

			fr.remove()
			fr = nil
		}

		mem = memdb.New(icmp)

		for r.journal.Next() {
			if err = batch.decode(r.journal.Record()); err != nil {
				return err
			}

			if err = batch.memReplay(mem); err != nil {
				return err
			}

			d.seq = batch.seq + uint64(batch.len())

			if mem.Size() > s.o.GetWriteBuffer() {
				// flush to table
				if err = cm.flush(mem, 0); err != nil {
					return err
				}

				// create new memdb
				mem = memdb.New(icmp)
			}
		}

		if err = r.journal.Error(); err != nil {
			return err
		}

		r.close()
		fr = r
	}

	// create new journal
	if _, err := d.newMem(); err != nil {
		return err
	}

	if mem != nil && mem.Len() > 0 {
		if err := cm.flush(mem, 0); err != nil {
			return err
		}
	}

	if err := cm.commit(d.journal.file.Num(), d.seq); err != nil {
		return err
	}

	if fr != nil {
		fr.remove()
	}

	return nil
}

// GetOptionsSetter return OptionsSetter for this database. OptionsSetter
// allows safely set options of an opened database.
func (d *DB) GetOptionsSetter() opt.OptionsSetter {
	return d.s.o
}

func (d *DB) get(key []byte, seq uint64, ro *opt.ReadOptions) (value []byte, err error) {
	s := d.s

	ucmp := s.cmp.cmp
	ikey := newIKey(key, seq, tSeek)

	memGet := func(m *memdb.DB) bool {
		var k []byte
		k, value, err = m.Find(ikey)
		if err != nil {
			return false
		}
		ik := iKey(k)
		if ucmp.Compare(ik.ukey(), key) != 0 {
			return false
		}
		if _, t, ok := ik.parseNum(); ok {
			if t == tDel {
				value = nil
				err = errors.ErrNotFound
			}
			return true
		}
		return false
	}

	mem := d.getMem()
	if memGet(mem.cur) || (mem.froze != nil && memGet(mem.froze)) {
		return value, err
	}

	value, cState, err := s.version().get(ikey, ro)

	if cState && !d.isClosed() {
		// schedule compaction
		select {
		case d.cch <- cSched:
		default:
		}
	}

	return value, err
}

// Get get value for given key of the latest snapshot of database.
func (d *DB) Get(key []byte, ro *opt.ReadOptions) ([]byte, error) {
	if err := d.rok(); err != nil {
		return nil, err
	}

	value, err := d.get(key, d.getSeq(), ro)
	if ro.HasFlag(opt.RFDontCopyBuffer) {
		return value, err
	}
	return dupBytes(value), err
}

// NewIterator return an iterator over the contents of the latest snapshot of
// database. The result of NewIterator() is initially invalid (caller must
// call Next or one of Seek method, i.e. First, Last or Seek).
//
// Please note that the iterator is not thread-safe, you may not use same
// iterator instance concurrently without external synchronization.
func (d *DB) NewIterator(ro *opt.ReadOptions) iterator.Iterator {
	if err := d.rok(); err != nil {
		return &iterator.EmptyIterator{err}
	}

	p := d.newSnapshot()
	i := p.NewIterator(ro)
	x, ok := i.(*dbIter)
	if ok {
		runtime.SetFinalizer(x, func(x *dbIter) {
			p.Release()
		})
	} else {
		p.Release()
	}
	return i
}

// GetSnapshot return a handle to the current DB state.
// Iterators created with this handle will all observe a stable snapshot
// of the current DB state. The caller must call *Snapshot.Release() when the
// snapshot is no longer needed.
func (d *DB) GetSnapshot() (*Snapshot, error) {
	if err := d.rok(); err != nil {
		return nil, err
	}

	snap := d.newSnapshot()
	runtime.SetFinalizer(snap, (*Snapshot).Release)
	return snap, nil
}

// GetProperty used to query exported database state.
//
// Valid property names include:
//
//  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
//     where <N> is an ASCII representation of a level number (e.g. "0").
//  "leveldb.stats" - returns a multi-line string that storribes statistics
//     about the internal operation of the DB.
//  "leveldb.sstables" - returns a multi-line string that storribes all
//     of the sstables that make up the db contents.
func (d *DB) GetProperty(prop string) (value string, err error) {
	if err = d.rok(); err != nil {
		return "", err
	}

	const prefix = "leveldb."
	if !strings.HasPrefix(prop, prefix) {
		return "", errors.ErrInvalid("unknown property: " + prop)
	}

	p := prop[len(prefix):]

	switch s := d.s; true {
	case strings.HasPrefix(p, "num-files-at-level"):
		var level uint
		var rest string
		n, _ := fmt.Scanf("%d%s", &level, &rest)
		if n != 1 || level >= kNumLevels {
			return "", errors.ErrInvalid("invalid property: " + prop)
		}
		value = fmt.Sprint(s.version().tLen(int(level)))
	case p == "stats":
		v := s.version()
		value = "Compactions\n" +
			" Level |   Tables   |    Size(MB)   |    Time(sec)  |    Read(MB)   |   Write(MB)\n" +
			"-------+------------+---------------+---------------+---------------+---------------\n"
		for level, tt := range v.tables {
			duration, read, write := d.cstats[level].get()
			if len(tt) == 0 && duration == 0 {
				continue
			}
			value += fmt.Sprintf(" %3d   | %10d | %13.5f | %13.5f | %13.5f | %13.5f\n",
				level, len(tt), float64(tt.size())/1048576.0, duration.Seconds(),
				float64(read)/1048576.0, float64(write)/1048576.0)
		}
	case p == "sstables":
		v := s.version()
		for level, tt := range v.tables {
			value += fmt.Sprintf("--- level %d ---\n", level)
			for _, t := range tt {
				value += fmt.Sprintf("%d:%d[%q .. %q]\n", t.file.Num(), t.size, t.min, t.max)
			}
		}
	default:
		return "", errors.ErrInvalid("unknown property: " + prop)
	}

	return value, nil
}

// GetApproximateSizes calculate approximate sizes of given ranges.
//
// Note that the returned sizes measure file system space usage, so
// if the user data compresses by a factor of ten, the returned
// sizes will be one-tenth the size of the corresponding user data size.
//
// The results may not include the sizes of recently written data.
func (d *DB) GetApproximateSizes(rr []Range) (Sizes, error) {
	if err := d.rok(); err != nil {
		return nil, err
	}

	v := d.s.version()
	sizes := make(Sizes, 0, len(rr))
	for _, r := range rr {
		min := newIKey(r.Start, kMaxSeq, tSeek)
		max := newIKey(r.Limit, kMaxSeq, tSeek)
		start, err := v.approximateOffsetOf(min)
		if err != nil {
			return nil, err
		}
		limit, err := v.approximateOffsetOf(max)
		if err != nil {
			return nil, err
		}
		var size uint64
		if limit >= start {
			size = limit - start
		}
		sizes = append(sizes, size)
	}

	return sizes, nil
}

// CompactRange compact the underlying storage for the key range.
//
// In particular, deleted and overwritten versions are discarded,
// and the data is rearranged to reduce the cost of operations
// needed to access the data.  This operation should typically only
// be invoked by users who understand the underlying implementation.
//
// Range.Start==nil is treated as a key before all keys in the database.
// Range.Limit==nil is treated as a key after all keys in the database.
// Therefore calling with Start==nil and Limit==nil will compact entire
// database.
func (d *DB) CompactRange(r Range) error {
	err := d.wok()
	if err != nil {
		return err
	}

	req := &cReq{level: -1}
	req.min = r.Start
	req.max = r.Limit

	d.creq <- req
	d.cch <- cWait

	return d.wok()
}

// Close closes the database. Snapshot and iterator are invalid
// after this call
func (d *DB) Close() error {
	if !d.setClosed() {
		return errors.ErrClosed
	}

	d.wlock <- struct{}{}
drain:
	for {
		select {
		case <-d.wqueue:
			d.wack <- errors.ErrClosed
		default:
			break drain
		}
	}
	close(d.wlock)

	// wake journal writer goroutine
	d.jch <- nil

	// wake Compaction goroutine
	d.cch <- cClose

	// wait for the WaitGroup
	d.ewg.Wait()

	// close journal
	if d.journal != nil {
		d.journal.close()
	}

	// close session
	d.s.close()

	if d.closeCb != nil {
		cerr := d.closeCb()
		if err := d.geterr(); err != nil {
			return err
		}
		return cerr
	}

	return d.geterr()
}
