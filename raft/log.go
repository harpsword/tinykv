// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/jinzhu/copier"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	lastIndex uint64
	offset    uint64 // snapShot.Metadata.Index + 1
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	stabled, _ := storage.LastIndex()
	log := &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   0,
		stabled:   stabled,
		lastIndex: stabled,
		offset:    1,
	}
	log.entries, _ = storage.Entries(1, stabled+1)
	return log
}

func (l *RaftLog) Check(entries []*pb.Entry) {
	for _, entryP := range entries {
		if entryP.Index <= 0 {
			continue
		}
		logTerm, err := l.Term(entryP.Index)
		if err != nil {
			//panic(err)
		}
		if logTerm != entryP.Term {
			l.Delete(entryP.Index)
			break
		}
	}
	l.stabled = min(l.stabled, l.LastIndex())
}

func (l *RaftLog) appendEntries(entry *pb.Entry) {
	l.entries = append(l.entries, *entry)
	l.lastIndex++
}

// AppendSlice 增加entries到log中
func (l *RaftLog) AppendSlice(entries []*pb.Entry) {
	l.Check(entries)
	indexNow := l.LastIndex() + 1
	for _, entry := range entries {
		var tmpEntry pb.Entry
		copier.Copy(&tmpEntry, entry)
		if tmpEntry.Index <= 0 {
			tmpEntry.Index = indexNow
		}
		if tmpEntry.Index < indexNow {
			// 已经有的log，前面已经check过了，所以一定是相同的
			continue
		}
		//l.entries = append(l.entries, tmpEntry)
		l.appendEntries(&tmpEntry)
		indexNow++
	}
}

func (l *RaftLog) Append(entry *pb.Entry) {
	l.AppendSlice([]*pb.Entry{entry})
}

func (l *RaftLog) UpdateIndex(entry *pb.Entry) {
	for id, entry2 := range l.entries {
		if entry2.Index == entry.Index && entry2.Term != entry.Term {
			l.entries[id] = *entry
			if entry.Index <= l.stabled {
				l.stabled = max(entry.Index-1, 0)
			}
		}
	}
}

// Delete 删除 index之后的所有entry，包含index在内
// 但是不会操作storage中的
func (l *RaftLog) Delete(index uint64) {
	if index == 0 {
		l.entries = []pb.Entry{}
	}
	if len(l.entries) == 0 {
		return
	}
	if l.entries[0].Index > index {
		panic(errors.New(fmt.Sprintf("wrong index(%d) for deleting entries", index)))
		return
	}
	if l.entries[len(l.entries)-1].Index < index {
		return
	}
	for id, entry := range l.entries {
		if entry.Index == index {
			l.entries = l.entries[:id]
			l.lastIndex = index - 1
			return
		}
	}

}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	entries, _ := l.EntriesAssist(l.stabled+1, l.LastIndex()+1)
	return entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	entries, _ := l.EntriesAssist(l.applied+1, l.committed+1)
	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//if len(l.entries) > 0 {
	//	return l.entries[len(l.entries)-1].Index
	//}
	//return 0
	return l.lastIndex
}

func (l *RaftLog) LastTerm() uint64 {
	lastIndex := l.LastIndex()
	term, _ := l.Term(lastIndex)
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, errors.New("too large index for getting Term")
	}
	storageLastIndex, err := l.storage.LastIndex()
	if err != nil {
		return 0, err
	}
	if i > storageLastIndex {
		// 查看 entries中的数据
		//firstIndex := l.entries[0].Index
		//return l.entries[i-firstIndex].Term, nil
		return l.TermOfEntries(i)
	}
	return l.storage.Term(i)
}

func (l *RaftLog) TermOfEntries(i uint64) (uint64, error) {
	if i < l.offset {
		if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		return 0, errors.New("too large index for getting Term")
	}
	lastIndex := l.LastIndex()
	if i > lastIndex {
		return 0, errors.New("too large index for getting Term")
	}
	return l.entries[i-l.offset].Term, nil
}

// isLogUpToDate 判断(mTerm, mIndex)是否比 l 要更新或者一样新
func (l *RaftLog) isLogUpToDate(mTerm uint64, mIndex uint64) bool {
	myIndex := l.LastIndex()
	myTerm, _ := l.Term(myIndex)
	if myTerm < mTerm {
		return true
	} else if myTerm == mTerm {
		return myIndex <= mIndex
	} else {
		return false
	}
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (l *RaftLog) EntriesFromStorageAssist(lo, hi uint64) ([]pb.Entry, error) {
	entries, err := l.storage.Entries(lo, hi)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (l *RaftLog) EntriesFromLocalAssist(lo, hi uint64) ([]pb.Entry, error) {
	for id, entry := range l.entries {
		if entry.Index == lo {
			return l.entries[id : uint64(id)+hi-lo], nil
		}
	}
	return nil, errors.New("cannot found index in RaftLog.entries")
}

func (l *RaftLog) EntriesFromLocal(lo, hi uint64) ([]*pb.Entry, error) {
	entries, err := l.EntriesFromLocalAssist(lo, hi)
	return l.EntrySlice2EntryPointerSlice(entries), err
}

// EntriesAssist 获取index范围在[lo,hi)的entry
func (l *RaftLog) EntriesAssist(lo, hi uint64) ([]pb.Entry, error) {
	if l.LastIndex() < hi-1 {
		return nil, errors.New("requested entry slice [lo, hi) is out of range")
	}
	storageLastIndex, err := l.storage.LastIndex()
	if err != nil {
		return nil, err
	}
	if storageLastIndex+1 >= hi {
		return l.EntriesFromStorageAssist(lo, hi)
	} else if lo > storageLastIndex {
		return l.EntriesFromLocalAssist(lo, hi)
	} else {
		entries1, err := l.EntriesFromStorageAssist(lo, storageLastIndex+1)
		if err != nil {
			return nil, err
		}
		entries2, err := l.EntriesFromLocalAssist(storageLastIndex+1, hi)
		if err != nil {
			return nil, err
		}
		entries1 = append(entries1, entries2...)
		return entries1, nil
	}
}

func (l *RaftLog) Entries(lo, hi uint64) ([]*pb.Entry, error) {
	entries, err := l.EntriesAssist(lo, hi)
	return l.EntrySlice2EntryPointerSlice(entries), err
}

func (l *RaftLog) EntrySlice2EntryPointerSlice(entries []pb.Entry) []*pb.Entry {
	entryPointSlice := make([]*pb.Entry, 0, len(entries))
	for id, _ := range entries {
		entryPointSlice = append(entryPointSlice, &entries[id])
	}
	return entryPointSlice
}

func (l *RaftLog) CommitTo(index uint64) {
	if index > l.committed {
		l.committed = index
	}
}

func (l *RaftLog) AppliedToSM(i uint64) {
	if i > l.applied {
		l.applied = i
	}
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) restore(s *pb.Snapshot) {
	l.committed = s.Metadata.Index
	l.pendingSnapshot = s
	l.entries = nil
	l.stabled = s.Metadata.Index
	l.lastIndex = s.Metadata.Index
	l.offset = s.Metadata.Index + 1
}

func (l *RaftLog) hasPendingSnapshot() bool {
	return l.pendingSnapshot != nil
}

func (l *RaftLog) hasNextEnts() bool {
	return l.committed > l.applied
}

func (l *RaftLog) stableSnapTo(index uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == index {
		l.pendingSnapshot = nil
	}
}
