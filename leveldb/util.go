// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"encoding/binary"
	"io"
	"sort"

	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type readByteReader interface {
	io.Reader
	io.ByteReader
}

func dupBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	r := make([]byte, len(b))
	copy(r, b)
	return r
}

func sliceBytes(b []byte) []byte {
	z, n := binary.Uvarint(b)
	return b[n : n+int(z)]
}

func sliceBytesTest(b []byte) (valid bool, v, rest []byte) {
	z, n := binary.Uvarint(b)
	m := n + int(z)
	if n <= 0 || m > len(b) {
		return false, nil, nil
	}
	return true, b[n:m], b[m:]
}

func readBytes(r readByteReader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil || n <= 0 {
		return nil, err
	}
	b := make([]byte, n)
	if _, err = io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func shorten(str string) string {
	if len(str) <= 13 {
		return str
	}
	return str[:5] + "..." + str[len(str)-5:]
}

type files []storage.File

func (p files) Len() int {
	return len(p)
}

func (p files) Less(i, j int) bool {
	return p[i].Num() < p[j].Num()
}

func (p files) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p files) sort() {
	sort.Sort(p)
}

type journalReader struct {
	file    storage.File
	reader  storage.Reader
	journal *journal.Reader
}

func newJournalReader(file storage.File, checksum bool, dropf journal.DropFunc) (*journalReader, error) {
	r, err := file.Open()
	if err != nil {
		return nil, err
	}
	jr, err := journal.NewReader(r, 0, checksum, dropf)
	if err != nil {
		return nil, err
	}
	return &journalReader{
		file:    file,
		reader:  r,
		journal: jr,
	}, nil
}

func (r *journalReader) closed() bool {
	return r.reader == nil
}

func (r *journalReader) close() {
	if r.closed() {
		return
	}
	r.reader.Close()
	r.reader = nil
	r.journal = nil
}

func (r *journalReader) remove() error {
	r.close()
	return r.file.Remove()
}

type journalWriter struct {
	file    storage.File
	writer  storage.Writer
	journal *journal.Writer
}

func newJournalWriter(file storage.File) (w *journalWriter, err error) {
	w = new(journalWriter)
	w.file = file
	w.writer, err = file.Create()
	if err != nil {
		return nil, err
	}
	w.journal = journal.NewWriter(w.writer)
	return w, nil
}

func (w *journalWriter) closed() bool {
	return w.writer == nil
}

func (w *journalWriter) close() {
	if w.closed() {
		return
	}
	w.writer.Close()
	w.writer = nil
	w.journal = nil
}

func (w *journalWriter) remove() error {
	w.close()
	return w.file.Remove()
}
