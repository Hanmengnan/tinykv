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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
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

	first uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, _ := storage.InitialState()
	first, _ := storage.FirstIndex()
	last, _ := storage.LastIndex()
	entries, _ := storage.Entries(first, last+1) // [first , last]

	return &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   first - 1, // 上文注释中提到：刚刚commits和applied到最新的快照中，因此applied指向的就是比storage中再older的上一条
		stabled:   last,
		entries:   entries,
		first:     first,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	storageFirstIndex, _ := l.storage.FirstIndex()
	if l.first < storageFirstIndex {
		compactedEntries := l.entries[storageFirstIndex-l.first:]
		l.entries = make([]pb.Entry, len(compactedEntries))
		copy(l.entries, compactedEntries)
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		index := int(l.stabled + 1 - l.first) // stabled+1为unstabled的第一条日志项的索引
		if index < 0 || index > len(l.entries) {
			return []pb.Entry{}
		} else {
			return l.entries[index:]
		}
	}
	return []pb.Entry{}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.first+1 : l.committed-l.first+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	} else {
		return l.stabled
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	last := l.LastIndex()
	if i > last {
		return 0, ErrUnavailable
	}
	if i < l.first {
		return l.storage.Term(i)
	} else {
		index := i - l.first
		return l.entries[index].Term, nil
	}
}

func (l *RaftLog) MaybeAppend(index, logTerm uint64, ents []*pb.Entry) (uint64, bool) {
	if index >= l.first {
		wantTerm, _ := l.Term(index)
		// 进行匹配
		if wantTerm == logTerm {
			//log.Printf("%-10s: index term is matched.", "[MAPPEND]")
		} else {
			// 不匹配说明从wantTerm开始的日志就是错误的，需要找到wantTerm的第一条日志重新匹配

			for i := l.first; i < index; i++ {
				if term, err := l.Term(i); err != nil && term == wantTerm {
					return i, false
				}
			}
			return l.first, false
		}
	}
	// 此时为下列情况之一
	// 1. msg.index< l.firstIndex 无法匹配
	// 2. msg.index 匹配成功
	// 将后续日志进行扩展
	// raft协议假定，当前这条日志匹配的情况下，这条日志之前的所有日志都匹配

	for _, ent := range ents {
		if ent.Index < l.first {
			continue
		}
		if ent.Index <= l.LastIndex() {
			if term, _ := l.Term(ent.Index); term != ent.Term {
				l.entries[ent.Index-l.first] = *ent
				l.entries = l.entries[:ent.Index-l.first+1]
				l.stabled = min(l.stabled, ent.Index-1)
			}
		} else {
			l.entries = append(l.entries, *ent)
		}
	}
	return l.LastIndex(), true
}

func (l *RaftLog) findConflictByTerm(index uint64, term uint64) uint64 {
	if li := l.LastIndex(); index > li {
		// NB: such calls should not exist, but since there is a straightfoward
		// way to recover, do it.
		//
		// It is tempting to also check something about the first index, but
		// there is odd behavior with peers that have no log, in which case
		// lastIndex will return zero and firstIndex will return one, which
		// leads to calls with an index of zero into this method.
		log.Panicf("%-10s: index(%d) is out of range [0, lastIndex(%d)] in findConflictByTerm", "[ERROR]",
			index, li)
		return index
	}
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}

func (l *RaftLog) matchTerm(index, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		log.Printf("term of index %d is unavailable， first index is %d ,last is %d", index, l.first, l.LastIndex())
		return false
	}
	if t != term {
		log.Printf("term is not equal, got is %d, want is %d", t, term)
	}

	return t == term
}
