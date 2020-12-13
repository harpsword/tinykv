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
	// random version of electionTimeout
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
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
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	raftNode := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		State:            StateFollower,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		votes:            map[uint64]bool{},
		msgs:             []pb.Message{},
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   None,
		PendingConfIndex: None,

		randomizedElectionTimeout: c.ElectionTick,
	}
	for _, id := range c.peers {
		raftNode.Prs[id] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
	return raftNode
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}
	progress, ok := r.Prs[to]
	if !ok {
		panic(errors.New("wrong id"))
	}
	preLogIndex := progress.Next - 1
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		panic(err.Error())
	}
	newMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Commit:  r.RaftLog.committed,
	}
	if r.RaftLog.LastIndex() >= progress.Next {
		newMsg.Entries, err = r.RaftLog.Entries(progress.Next, r.RaftLog.LastIndex()+1)
		if err != nil {
			panic(err)
		}
	}
	r.msgs = append(r.msgs, newMsg)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

func (r *Raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
		})
	}
}

func (r *Raft) tickHeartbeat() {
	r.electionElapsed++
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			Term:    r.Term,
			To:      r.id,
		})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Reset(r.Term + 1)
	r.Vote = r.id
	r.votes[r.id] = true
}

func (r *Raft) Reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	// reset Prs
	r.ResetVotes()
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
	//r.leadTransferee = None
	//r.PendingConfIndex = None
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *Raft) ResetVotes() {
	r.votes = map[uint64]bool{}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Reset(r.Term)
	r.State = StateLeader

	//r.msgs = append(r.msgs, pb.Message{
	//	MsgType: pb.MessageType_MsgBeat,
	//	To:      r.id,
	//	From:    r.id,
	//})
	for id, _ := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0
	}
	//r.RaftLog.Append(&pb.Entry{
	//	Index: r.RaftLog.LastIndex() + 1,
	//	Term:  r.Term,
	//})
	r.Step(pb.Message{
		From:    r.id,
		To:      r.id,
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&pb.Entry{}},
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	switch {
	case m.Term == 0:
		// Local Message

	case m.Term > r.Term:
		// 需要变成follower
		if IsLeaderSendMsg(m.MsgType) {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term:
		// 需要返回对应的Response
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				Term:    r.Term,
				From:    r.id,
				To:      m.From,
			})
			return nil
		case pb.MessageType_MsgAppend:
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				Term:    r.Term,
				From:    r.id,
				To:      m.From,
			})
			return nil
		}
	}
	// 对于m.Term > r.Term的情况，r.Term=m.Term,并且State为Follower
	// 对于m.Term < r.Term的情况，部分已经处理
	// 剩下只有r.Term=m.Term的情况
	// 所以之后的处理中 m.Term <= r.Term
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// local message
		r.hup()
	case pb.MessageType_MsgBeat:
		// local message
		if r.State == StateLeader {
			for id, _ := range r.Prs {
				if id != r.id {
					r.sendHeartbeat(id)
				}
			}
		}
	default:
		// deal with Message
		switch r.State {
		case StateLeader:
			return r.stepLeader(m)
		case StateFollower:
			return r.stepFollower(m)
		case StateCandidate:
			return r.stepCandidate(m)
		}
	}
	return nil
}

func (r *Raft) hup() {
	if r.State == StateLeader {
		return
	}
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	for id, _ := range r.Prs {
		if id != r.id {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				To:      id,
				From:    r.id,
				Term:    r.Term,
				Index:   r.RaftLog.LastIndex(),
				LogTerm: r.RaftLog.LastTerm(),
			})
		}
	}
}

func (r *Raft) stepLeader(m pb.Message) (err error) {
	if r.State != StateLeader {
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			r.Prs[m.From].Next = max(r.Prs[m.From].Next-1, 0)
			r.sendAppend(m.From)
		} else {
			//r.Prs[m.From].Next = min(r.Prs[m.From].Next+1, r.RaftLog.LastIndex()+1)
			//r.Prs[m.From].Match = r.Prs[m.From].Next - 1
			r.Prs[m.From].Next = min(m.Index+1, r.RaftLog.LastIndex()+1)
			r.Prs[m.From].Match = r.Prs[m.From].Next - 1
			r.CommitEntries()
		}
	case pb.MessageType_MsgPropose:
		for _, entry := range m.Entries {
			entry.Term = r.Term
			r.RaftLog.Append(entry)
		}
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[r.id].Match = r.Prs[r.id].Next - 1
		for id, _ := range r.Prs {
			if id != r.id {
				// TODO: change to async
				r.sendAppend(id)
			}
		}
		if len(r.Prs) == 1 {
			// no follower or candidate need to sync entries
			r.CommitEntries()
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return
}

func (r *Raft) CommitEntries() {
	for commitIndex := r.RaftLog.LastIndex(); commitIndex > r.RaftLog.committed; commitIndex-- {
		term, err := r.RaftLog.Term(commitIndex)
		if err != nil {
			panic(err)
		}
		if term < r.Term {
			// 只commit当前term的log entry
			break
		}

		if r.IsHalfMatched(commitIndex) {
			r.RaftLog.CommitTo(commitIndex)
			// 给其他节点发消息，表示可以commit了
			r.Step(pb.Message{
				From:    r.id,
				To:      r.id,
				MsgType: pb.MessageType_MsgPropose,
				Entries: []*pb.Entry{},
			})
			return
		}
	}
}

func (r *Raft) IsHalfMatched(index uint64) bool {
	count := 1
	all := len(r.Prs)
	for id, prog := range r.Prs {
		if id != r.id {
			if prog.Match >= index {
				count++
				if AboveHalf(count, all) {
					return true
				}
			}
		}
	}
	return AboveHalf(count, all)
}

func (r *Raft) stepFollower(m pb.Message) (err error) {
	if r.State != StateFollower {
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return
}

func (r *Raft) stepCandidate(m pb.Message) (err error) {
	if r.State != StateCandidate {
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVoteResponse:
		// 检查票数
		r.votes[m.From] = !m.Reject
		// 包含自己的一票
		count := 0
		all := len(r.Prs)
		for _, vote := range r.votes {
			if vote {
				count++
			}
		}
		if count*2 > all {
			// 判断是否拿到了一半的票
			r.becomeLeader()
		} else if all == len(r.votes) {
			// 当所有node投票完成之后，还没有拿到一半的票，转变为Follower
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return
}

func (r *Raft) handleRequestVote(m pb.Message) {
	// 来自candidate的请求投票
	if r.State == StateCandidate || r.State == StateLeader {
		r.sendResponse(pb.MessageType_MsgRequestVoteResponse, m.From, true, 0)
		return
	}

	if (r.Vote == None || r.Vote == m.From) && r.Term <= m.Term && r.isLogUpToDate(m.LogTerm, m.Index) {
		// 判断是否能给candidate投票
		r.Vote = m.From
		r.sendResponse(pb.MessageType_MsgRequestVoteResponse, m.From, false, 0)
	} else {
		r.sendResponse(pb.MessageType_MsgRequestVoteResponse, m.From, true, 0)
	}
}

func (r *Raft) isLogUpToDate(mTerm uint64, mIndex uint64) bool {
	return r.RaftLog.isLogUpToDate(mTerm, mIndex)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	r.Lead = m.From

	rPrevTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		r.sendResponse(pb.MessageType_MsgAppendResponse, m.From, true, 0)
		return
	}
	if rPrevTerm != m.LogTerm {
		r.sendResponse(pb.MessageType_MsgAppendResponse, m.From, true, 0)
		return
	}

	// 内部会先进行校验，后添加到entries中
	r.RaftLog.AppendSlice(m.Entries)

	if m.Commit > r.RaftLog.committed {
		newCommitIndex := m.Index
		if len(m.Entries) > 0 {
			newCommitIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.CommitTo(min(m.Commit, newCommitIndex))
	}
	// 当返回AppendResponse时，reject=false，此时 Index表示 新的Match index
	r.sendResponse(pb.MessageType_MsgAppendResponse, m.From, false, r.RaftLog.LastIndex())

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.becomeFollower(m.Term, m.From)
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	})
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

func (r *Raft) sendResponse(msgType pb.MessageType, to uint64, reject bool, index uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: msgType,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
		Index:   index,
	})
}
