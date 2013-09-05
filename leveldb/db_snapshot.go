// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type snapEntry struct {
	elem *list.Element
	seq  uint64
	ref  int
}

type snaps struct {
	sync.Mutex
	list.List
}

// Create new initaliized snaps object.
func newSnaps() *snaps {
	p := new(snaps)
	p.Init()
	return p
}

// Insert given seq to the list.
func (p *snaps) acquire(seq uint64) (e *snapEntry) {
	p.Lock()
	if back := p.Back(); back != nil {
		e = back.Value.(*snapEntry)
	}
	if e == nil || e.seq != seq {
		e = &snapEntry{seq: seq}
		e.elem = p.PushBack(e)
	}
	e.ref++
	p.Unlock()
	return e
}

// Release given entry; remove it when ref reach zero.
func (p *snaps) release(e *snapEntry) {
	p.Lock()
	e.ref--
	if e.ref == 0 {
		p.Remove(e.elem)
	}
	p.Unlock()
}

// Get smallest sequence or return given seq if list empty.
func (p *snaps) seq(seq uint64) uint64 {
	p.Lock()
	defer p.Unlock()
	if back := p.Back(); back != nil {
		return back.Value.(*snapEntry).seq
	}
	return seq
}

// Snapshot represent a database snapshot.
type Snapshot struct {
	d        *DB
	entry    *snapEntry
	released uint32
}

// Create new snapshot object.
func (d *DB) newSnapshot() *Snapshot {
	return &Snapshot{d: d, entry: d.snaps.acquire(d.getSeq())}
}

func (p *Snapshot) isOk() bool {
	if atomic.LoadUint32(&p.released) != 0 {
		return false
	}
	return !p.d.isClosed()
}

func (p *Snapshot) ok() error {
	if atomic.LoadUint32(&p.released) != 0 {
		return errors.ErrSnapshotReleased
	}
	return p.d.rok()
}

// Get get value for given key of this snapshot of database.
func (p *Snapshot) Get(key []byte, ro *opt.ReadOptions) ([]byte, error) {
	if atomic.LoadUint32(&p.released) != 0 {
		return nil, errors.ErrSnapshotReleased
	}

	d := p.d

	if err := d.rok(); err != nil {
		return nil, err
	}

	return d.get(key, p.entry.seq, ro)
}

// NewIterator return an iterator over the contents of this snapshot of
// database.
//
// Please note that the iterator is not thread-safe, you may not use same
// iterator instance concurrently without external synchronization.
func (p *Snapshot) NewIterator(ro *opt.ReadOptions) iterator.Iterator {
	if atomic.LoadUint32(&p.released) != 0 {
		return &iterator.EmptyIterator{errors.ErrSnapshotReleased}
	}

	d := p.d

	if err := d.rok(); err != nil {
		return &iterator.EmptyIterator{err}
	}

	return &dbIter{
		snap:       p,
		cmp:        d.s.cmp.cmp,
		it:         d.newRawIterator(ro),
		seq:        p.entry.seq,
		copyBuffer: !ro.HasFlag(opt.RFDontCopyBuffer),
	}
}

// Release release the snapshot. The caller must not use the snapshot
// after this call.
func (p *Snapshot) Release() {
	if atomic.CompareAndSwapUint32(&p.released, 0, 1) {
		p.d.snaps.release(p.entry)
		p.d = nil
		p.entry = nil
	}
}
