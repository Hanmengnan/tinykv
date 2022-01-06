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
	"log"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err.Error())
	}

	progresses := make(map[uint64]*Progress)
	votes := make(map[uint64]bool)

	node := Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              progresses,
		State:            StateFollower,
		votes:            votes,
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		heartbeatElapsed: 0,
		electionTimeout:  c.ElectionTick,
		electionElapsed:  0,
	}

	if c.peers == nil {
		c.peers = confState.Nodes
	}
	lastIndex := node.RaftLog.LastIndex()
	for _, item := range c.peers {
		node.Prs[item] = &Progress{Next: lastIndex + 1, Match: 0}
	}

	return &node
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		return false
	}

	entities := make([]*pb.Entry, 0)
	for i := prevLogIndex + 1; i <= r.RaftLog.LastIndex(); i++ {
		entities = append(entities, &r.RaftLog.entries[i-r.RaftLog.FirstIndex()])
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   prevLogIndex,
		LogTerm: prevLogTerm,
		Entries: entities,
		Commit:  r.RaftLog.committed,
	}
	// log.Printf("i am node %d, i will send append to %d,content is %v", r.id, to, entities)
	r.msgs = append(r.msgs, msg)
	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// log.Printf("i am node %d, i recieve msg from %d", r.id, m.From)
	// 过时的消息
	if m.Term < r.Term {
		// log.Printf("node %d reject append, because m's term is %d", r.id, m.Term)
		r.sendAppendResponse(m.From, true)
		return
	} else {
		// m.Term>=r.Term直接更新，index可能存在落后的情况，但是目前m.From发送了心跳，因此还承认m.From是leader
		r.becomeFollower(m.Term, m.From)

		lastIndex := r.RaftLog.LastIndex()
		// entry 出现漏洞
		if m.Index > lastIndex {
			// log.Printf("i am node %d, exist hole", r.id)
			r.sendAppendResponse(m.From, true)
			return
		}

		LogTerm, err := r.RaftLog.Term(m.Index)
		// 本地没找到该Index的Term,告知leader以便补齐前面的日志
		if err != nil && err == ErrCompacted {
			r.sendAppendResponse(m.From, false)
			return
		}

		// 消息Term与本地记录不一致
		if LogTerm != m.LogTerm {
			r.sendAppendResponse(m.From, true)
			return
		}

		if len(m.Entries) > 0 {
			// 开始补齐日志，补齐日志存在两种情况：
			// 1. 存在异常日志，也即相同的index但term不同，则包括出现异常的日志及以后的日志，都需要补充
			// 2。存在缺少的日志，补充缺少的日志
			// 也即是找到需要补充的第一条日志的索引值
			index := 0
			for _, mitem := range m.Entries {
				// 存在本地未同步的消息，后续的消息都需要同步
				if mitem.Index > lastIndex {
					break
				}
				term, err := r.RaftLog.Term(uint64(mitem.Index))
				// 存在不一致的消息，后续消息都不需要考虑了
				if err == nil && term != mitem.Term {
					r.RaftLog.entries = r.RaftLog.entries[:mitem.Index-r.RaftLog.FirstIndex()]
					r.RaftLog.stabled = m.Index
					// log.Printf("entry whose index is %d have occurs with node %d", mitem.Index, r.id)
					break
				}
				index++
			}
			// 补充日志
			for i := index; i < len(m.Entries); i++ {
				e := m.Entries[i]
				r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
					EntryType: e.EntryType,
					Term:      e.Term,
					Index:     e.Index,
					Data:      e.Data,
				})
			}
		}
		// log.Printf("i am node %d, committed is %d", r.id, r.RaftLog.committed)
		if m.Commit > r.RaftLog.committed {
			committed := min(m.Commit, m.Index+uint64(len(m.Entries)))
			r.RaftLog.committed = min(committed, r.RaftLog.LastIndex())
		}
		// log.Printf("i am node %d, committed is %d", r.id, r.RaftLog.committed)
		r.sendAppendResponse(m.From, false)
	}
}

func (r *Raft) sendAppendResponse(from uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      from,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Reject:  reject,
	}
	// log.Printf("i am node %d, my last index is %d", r.id, r.RaftLog.LastIndex())
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
		return
	} else if r.Term == m.Term {
		if m.Reject {
			r.Prs[m.From].Next = r.Prs[m.From].Next - 1
			r.sendAppend(m.From)
		} else {
			r.Prs[m.From].Next = m.Index + 1
			r.Prs[m.From].Match = m.Index
		}
	}
	// log.Printf("i am node %d, i receive msg from node %d, its commit is %d, my commit is %d", r.id, m.From, m.Index, r.RaftLog.committed)
	r.advanceCommit()
}

func (r *Raft) advanceCommit() {
	// log.Printf("i am node %d, my last index is %d, my commit is %d", r.id, r.RaftLog.LastIndex(), r.RaftLog.committed)

	lastIndex := r.RaftLog.LastIndex()
	newCommit := false
	for i := r.RaftLog.committed + 1; i <= lastIndex; i += 1 {
		term, _ := r.RaftLog.Term(i)
		if term != r.Term {
			continue
		}

		n := 0
		for _, p := range r.Prs {
			if p.Match >= i {
				n += 1
			}
		}
		if n*2 > len(r.Prs) && r.RaftLog.committed < i {
			r.RaftLog.committed = i
			newCommit = true
		}
	}

	if newCommit {
		for peer := range r.Prs {
			if peer != r.id {
				r.sendAppend(peer)
			}
		}
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	// prevLogIndex := r.RaftLog.LastIndex()
	// prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	// if err != nil {
	// 	panic(err.Error())
	// }

	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	// log.Printf("send heartbeat to %d", to)
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// log.Printf("i am node %d, my committed is %d, leader committed is %d", r.id, r.RaftLog.committed, m.Commit)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   r.RaftLog.stabled,
	}
	if m.Term < r.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Term > r.Term {
		msg.Reject = false
		r.Term = m.Term
	}
	if m.From != r.Lead {
		r.Lead = m.From
	}
	r.heartbeatElapsed = 0
	r.resetElectionTime()
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote() {
	log.Printf("i am node %d, i start sending RequestVote msg", r.id)
	if r.State == StateCandidate {
		prevLogIndex := r.RaftLog.LastIndex()
		prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
		log.Printf("my peers is %+v", r.Prs)
		for index := range r.Prs {
			if index != r.id {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					From:    r.id,
					To:      index,
					Term:    r.Term,
					LogTerm: prevLogTerm,
					Index:   prevLogIndex,
					Commit:  r.RaftLog.committed,
				})
			}
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	log.Printf("i am node %d term %d, now node %d term %d index %d request vode", r.id, r.Term, m.From, m.Term, m.Index)

	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	} else if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
	}
	if r.Vote == None || r.Vote == m.From {
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		if lastTerm < m.LogTerm || (lastTerm == m.LogTerm && m.Index >= lastIndex) {
			r.Vote = m.From
			r.sendRequestVoteResponse(m.From, false)
			log.Printf("my vote for %d", m.From)
			return
		}
	}
	log.Printf("my don't vote for %d", m.From)
	r.sendRequestVoteResponse(m.From, true)

}

func (r *Raft) sendRequestVoteResponse(from uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      from,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	// 投票情况
	voteForMe, rejectMe := 0, 0
	for _, v := range r.votes {
		if v {
			voteForMe++
		} else {
			rejectMe++
		}
	}

	if voteForMe*2 > len(r.Prs) {
		r.becomeLeader()
	} else if rejectMe*2 >= len(r.Prs) {
		r.becomeFollower(r.Term, m.From)
	}

}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	switch r.State {
	case StateFollower, StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
			r.resetElectionTime()
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.resetElectionTime()
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// 将ElectionTimeout重置为随机时间
func (r *Raft) resetElectionTime() {
	r.electionElapsed = -rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	for key := range r.votes {
		r.votes[key] = false
	}
	r.heartbeatElapsed = 0
	r.resetElectionTime()

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Lead = None
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true // 投票给自己
	r.heartbeatElapsed = 0
	r.resetElectionTime()
	if len(r.Prs) == 1 { //只有一个自己节点
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Printf("i have become leader, i am node %d", r.id)
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.resetElectionTime()

	lastIndex := r.RaftLog.LastIndex()
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: lastIndex + 1,
	})

	r.Prs[r.id] = &Progress{Match: lastIndex + 1, Next: lastIndex + 2}

	for index := range r.Prs {
		if index != r.id {
			r.Prs[index].Next = lastIndex + 1
			r.Prs[index].Match = 0
			r.sendAppend(index)
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed++
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleMsgUp()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleMsgUp()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for index := range r.Prs {
				if index != r.id {
					r.sendHeartbeat(index)
				}
			}
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		}
	}
	return nil
}

func (r *Raft) handleMsgUp() {
	log.Printf("I receive Msgup msg, i am node %d", r.id)
	r.becomeCandidate()
	log.Printf("i am node %d ,now in term %d ,i start vote", r.id, r.Term)
	r.sendRequestVote()
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Reject || m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	log.Printf("I receive Propose msg, i am node %d", r.id)
	log.Printf("my committed is %d", r.RaftLog.committed)
	for _, e := range m.Entries {
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
			EntryType: e.EntryType,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      e.Data,
		})
	}
	for index := range r.Prs {
		if index != r.id {
			r.sendAppend(index)
		} else {
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
			r.Prs[r.id].Next = r.Prs[r.id].Match + 1
			if len(r.Prs) == 1 {
				r.RaftLog.committed++
			}
		}
	}
	// log.Printf("my committed is %d", r.RaftLog.committed)
}

func (r *Raft) GetSoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) GetHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
