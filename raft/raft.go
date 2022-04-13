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
		log.Printf("%-10s: loading initial state from storage fail.", "[ERROR]")
		panic(err.Error())
	}

	node := Raft{
		id:               c.ID,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		heartbeatElapsed: 0,
		electionTimeout:  c.ElectionTick,
		electionElapsed:  0,
	}

	if len(confState.Nodes) != 0 {
		c.peers = confState.Nodes
	}
	// TODO: Next 的值为何需要为 lastIndex+1
	lastIndex := node.RaftLog.LastIndex()
	for _, item := range c.peers {
		node.Prs[item] = &Progress{Next: lastIndex + 1, Match: 0}
	}
	node.becomeFollower(0, None)

	if c.Applied > 0 {
		node.changeApplied(c.Applied)
	}

	node.loadHardState(hardState)

	return &node
}

// change RaftLog applied index
func (r *Raft) changeApplied(index uint64) {
	if index < r.RaftLog.committed || index < r.RaftLog.applied {
		log.Panicf("%-10s: new applied index is unvalid", "[PANIC]")
	}
	r.RaftLog.applied = index
}

// load hardState when create new Raft
func (r *Raft) loadHardState(hs pb.HardState) {
	if hs.Commit < r.RaftLog.committed || hs.Commit > r.RaftLog.LastIndex() {
		log.Panicf("%-10s: hardstate comitted index is unvalid", "[PANIC]")
	}
	r.RaftLog.committed = hs.Commit
	r.Term = hs.Term
	r.Vote = hs.Vote
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	//log.Printf("%-10s: i am node %d,i am %s", "[INFO]", r.id, stmap[r.State])
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		//log.Printf("%-10s: my electionElapsed is %d,my electionTimeout is %d", "[INFO]", r.electionElapsed, r.electionTimeout)
		if r.electionElapsed >= r.electionTimeout {
			r.resetElectionTime()
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
			})
		}
	case StateLeader:
		r.heartbeatElapsed++
		//log.Printf("%-10s: my heartbeatElapsed is %d,my heartbeatTimeout is %d", "[INFO]", r.heartbeatElapsed, r.heartbeatTimeout)
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			// 这里我觉得没必要重置选举时间，因为作为leader无需选举，降级为follower之后会重置选举时间，因此在leader状态下没必要记录选举时间
			r.resetHeartBeatTime()
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
			})
		}
	}
}

// 重置心跳包发送时间
func (r *Raft) resetHeartBeatTime() {
	r.heartbeatElapsed = 0
}

// 重置选举时间
func (r *Raft) resetElectionTime() {
	r.electionElapsed = -rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	//log.Printf("%-10s: i am node %d, i become follower.", "[BECOME]", r.id)
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	// TODO: 重置以下字段是否有必要
	r.Vote = None
	//r.votes = make(map[uint64]bool)

	// TODO: Prs,msgs字段是否需要处理？

	r.resetHeartBeatTime()
	r.resetElectionTime()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term = r.Term + 1
	r.Lead = None
	r.Vote = r.id

	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true // 投票给自己
	r.Vote = r.id

	r.resetHeartBeatTime()
	r.resetElectionTime()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	log.Printf("%-10s: i have become leader, i am node %d, my peers is %+v", "[INFO]", r.id, r.Prs)
	r.State = StateLeader
	r.Lead = r.id
	r.resetHeartBeatTime()
	r.resetElectionTime()
	// TODO: 这步到底加不加？
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
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
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleMsgUp()
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		// TODO: 还会响应其他节点的投票请求吗？
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			r.handleMsgBeat()
		case pb.MessageType_MsgPropose:
			r.handleMsgPropose(m)
		// 收到其他节点的Append消息，会降级自身身份
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequestVote(m)
		// 收到其他节点的心跳包，会降级自身身份
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		}
	}
	return nil
}

func (r *Raft) handleMsgUp() {
	//log.Printf("%-10s: i am node %d,I receive Msgup msg.", "[MSGUP]", r.id)
	r.becomeCandidate()
	if len(r.Prs) == 1 { //只有一个自己节点
		r.becomeLeader()
	} else {
		//log.Printf("%-10s: i am node %d ,now in term %d ,i start vote.", "[MSGUP]", r.id, r.Term)
		r.sendRequestVote()
	}

}

func (r *Raft) handleMsgBeat() {
	//log.Printf("%-10s: I receive MsyBeat msg, i am node %d", "[MSGBEAT]", r.id)
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	//log.Printf("%-10s: I receive Propose msg, i am node %d", "[PROPOSE]", r.id)
	//log.Printf("%-10s: my committed is %d", "[PROPOSE]", r.RaftLog.committed)
	for _, entry := range m.Entries {
		r.RaftLog.Append([]*pb.Entry{
			{
				EntryType: pb.EntryType_EntryNormal,
				Term:      r.Term,
				Index:     r.RaftLog.LastIndex() + 1,
				Data:      entry.Data,
			},
		})
	}
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		} else {
			r.Prs[id].Next = r.RaftLog.LastIndex() + 1
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed++
	}
	// log.Printf("my committed is %d", r.RaftLog.committed)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		log.Panicf("%-10s: a node can't sendAppend if it isn't leader.", "[ERROR]")
		return false
	}
	//log.Printf("%-10s: i am node %d, my entries is %+v", "[APPEND]", r.id, r.RaftLog.entries)

	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		log.Printf("%-10s: get prevLogTerm fail.", "[ERROR]")
	}

	entities := make([]*pb.Entry, 0)
	for i := r.Prs[to].Next; i <= r.RaftLog.LastIndex(); i++ {
		entities = append(entities, &r.RaftLog.entries[i-r.RaftLog.first])
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
	r.msgs = append(r.msgs, msg)
	//log.Printf("%-10s: i am node %d, i will send append to %d,msgs is %+v", "[APPEND]", r.id, to, r.msgs)
	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//log.Printf("%-10s: i am node %d, i receive msg from %d, msg is %+v", "[RAPPEND]", r.id, m.From, m)
	//log.Printf("%-10s: i am node %d, my entries are %+v", "[RAPPEND]", r.id, r.RaftLog.entries)
	if m.Term != None && m.Term < r.Term {
		r.sendAppendResponse(m.From, true, None, None)
		return
	}
	r.becomeFollower(m.Term, m.From)

	if index, ok := r.RaftLog.MaybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries); ok {
		//log.Printf("%-10s: i am node %d,i accept append.", "[ACCEPT]", r.id)
		logTerm, _ := r.RaftLog.Term(index)
		r.sendAppendResponse(m.From, false, index, logTerm)
	} else {
		// 删除index超过leader记录的日志
		if r.RaftLog.LastIndex() > m.Index {
			index = m.Index
		} else {
			index = r.RaftLog.LastIndex()
		}
		// 删除term超过leader记录的日志
		for ; index >= r.RaftLog.first; index-- {
			term, err := r.RaftLog.Term(index)
			if term <= m.LogTerm || err != nil {
				break
			}
		}
		//log.Printf("%-10s: i am node %d,i reject append.", "[REJECT]", r.id)
		logTerm, _ := r.RaftLog.Term(index)
		r.sendAppendResponse(m.From, true, index, logTerm)
	}
}

func (r *Raft) sendAppendResponse(from uint64, reject bool, lastLogIndex, lastLogTerm uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      from,
		From:    r.id,
		Term:    r.Term,
		Index:   lastLogIndex,
		LogTerm: lastLogTerm,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	//log.Printf("%-10s: i receive AppendResponse from %d", "[HRAPPEND]", m.From)
	if m.Reject {
		nextLogIndex := r.RaftLog.findConflictByTerm(m.LogTerm, m.Index)
		r.Prs[m.From].Next = max(nextLogIndex, r.RaftLog.first)
		r.Prs[m.From].Match = r.Prs[m.From].Next - 1
		r.sendAppend(m.From)
	} else {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	}

	r.advanceCommit()
	//log.Printf("%-10s: i am node %d, my commit is %d", "[HRAPPEND]", r.id, r.RaftLog.committed)
}

func (r *Raft) sendRequestVote() {
	//log.Printf("%-10s: i am node %d, i start sending RequestVote msg", "[SVOTE]", r.id)
	if r.State == StateCandidate {
		newestIndex := r.RaftLog.LastIndex()
		logTerm, _ := r.RaftLog.Term(newestIndex)
		for id := range r.Prs {
			if id != r.id {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgRequestVote,
					To:      id,
					From:    r.id,
					Term:    r.Term,
					LogTerm: logTerm,
					Index:   newestIndex,
					Commit:  r.RaftLog.committed,
				})
			}
		}
	} else {
		panic("%-10d: if a node is not candidate, it can't send RequestVote.")
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	//log.Printf("%-10s: i am node %d term %d my lastIndex is %d , now node %d logTerm %d index %d request vode", "[RVOTE]", r.id, r.Term, r.RaftLog.LastIndex(), m.From, m.LogTerm, m.Index)

	if m.Term < r.Term { // 请求投票的节点是过时的，直接拒绝
		//log.Printf("%-10s: i vote for %d", "[RVOTE]", m.From)
		r.sendRequestVoteResponse(m.From, true)
		return
	} else {
		var canVote bool
		if m.Term > r.Term { // 请求投票的任期更新，解除对上一轮投票节点的青睐
			r.becomeFollower(m.Term, None)
			canVote = true
		} else {
			canVote = r.Vote == None && r.Lead == None || r.Vote == m.From // 尚未为任何节点投票，则每个节点都可以获得我的支持。
		}
		lastIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastIndex)
		if canVote && (m.LogTerm > lastLogTerm || (m.LogTerm == lastLogTerm && m.Index >= lastIndex)) {
			r.becomeFollower(m.Term, None)
			r.sendRequestVoteResponse(m.From, false)
			//log.Printf("%-10s: i am node %d, i vote for %d", "[RVOTE]", r.id, m.From)
		} else {
			r.sendRequestVoteResponse(m.From, true)
			//log.Printf("%-10s: i am node %d, i don't vote for %d, because my lastIndex is %d, lastTerm is %d", "[RVOTE]", r.id, m.From, lastIndex, lastLogTerm)
		}

	}
}

func (r *Raft) sendRequestVoteResponse(from uint64, reject bool) {
	if !reject {
		r.Vote = from // 为from投票，记录作为下一轮的参考
	}
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
	if _, ok := r.votes[m.From]; !ok {
		r.votes[m.From] = !m.Reject
	}

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
	} else if rejectMe*2 > len(r.Prs) {
		r.becomeFollower(r.Term, None)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	//log.Printf("%-10s: send heartbeat to %d", "[HEATBEAT]", to)
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.becomeFollower(m.Term, m.From)
	r.resetHeartBeatTime()
	r.resetElectionTime()
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) sendHeartbeatResponse(id uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      id,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		Commit:  r.RaftLog.committed,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
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

func (r *Raft) advanceCommit() {
	lastIndex := r.RaftLog.LastIndex()
	newCommit := false

	for i := r.RaftLog.committed + 1; i <= lastIndex; i++ {
		term, _ := r.RaftLog.Term(i)
		if term != r.Term { // 不能提交不属于本次任期的日志
			continue
		}

		n := 0
		for id, p := range r.Prs {
			if r.id == id || p.Match >= i {
				n += 1
			}
		}

		if n*2 > len(r.Prs) {
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

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
