package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// Types of node states
//
const (
	STATE_CANDIDATE = iota
	STATE_FOLLOWER
	STATE_LEADER
)

type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	Command      interface{}

	UseSnapshot bool
	Snapshot    []byte
}

//
// Utility function
//
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state     int
	voteCount int

	// Vars for persistent states on all servers
	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// candidateId that received vote in current term (or null if none)
	votedFor int
	// log entries; each entry contains command for state machine, and term when entry was received by leader
	log []LogEntry

	// Vars for volatile state on all servers
	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	lastApplied int

	// Vars for volatile state on leader nodes
	// Reinitialized after each election
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int

	// Declared channels between raft peers
	chanWinElect  chan bool
	chanHeartbeat chan bool
	chanApply     chan ApplyMsg
	chanGiveVote  chan bool
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term         int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	NextTryIndex int
}

type InitiateSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InitiateSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

//
// Function snips the log entries up to lastIncludedIndex.
//
func (rf *Raft) ShortenLog(lastIncludedIndex int, lastIncludedTerm int) {
	updatedLog := make([]LogEntry, 0)
	updatedLog = append(updatedLog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			updatedLog = append(updatedLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = updatedLog
}

// In snapshotting, the entire current system state is written to a snapshot on stable storage,
// then the entire log up to that point is discarded
func (rf *Raft) SendInstallSnapshot(server int, args *InitiateSnapshotArgs, reply *InitiateSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Only accepts requests from servers with up to date term numbers
	if !ok || rf.state != STATE_LEADER || args.Term != rf.currentTerm {
		return ok
	}

	// Become a follower and update current term of server
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.Persist()
		return ok
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return ok
}

// InstallSnapshot is sent by leader to followers that are too far behind
// if the snapshot contain new information not already in the recipient’s log
// the follower discards its entire log; it is all superseded by the snapshot
// and may possibly have uncommitted entries that conflict with the snapshot.
// If instead the follower receives a snapshot that describes a prefix of its log
// (due to retransmission or by mistake), then log entries covered by the snapshot
// are deleted but entries following the snapshot are still valid and must be retained.
func (rf *Raft) InstallSnapshot(args *InitiateSnapshotArgs, reply *InitiateSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Only accepts requests from servers with up to date term numbers
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// Become a follower and update current term of server
	if args.Term > rf.currentTerm {
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.Persist()
	}

	// Checks heartbeat to restart timeout
	rf.chanHeartbeat <- true

	reply.Term = rf.currentTerm

	//Create new snapshot file if first chunk (offset is 0)
	// Save snapshot file, discard any existing or partial snapshot with a smaller index
	// If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	if args.LastIncludedIndex > rf.commitIndex {
		rf.ShortenLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.GetRaftState(), args.Data)

		// Transfers the snapshot over to kv
		msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
		rf.chanApply <- msg
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term := rf.currentTerm
	isleader := (rf.state == STATE_LEADER)
	return term, isleader
}

func (rf *Raft) GetLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) GetLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

//
// Function for getting the previously encoded raft state size
//
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// Function for encoding the current raft state.
//
func (rf *Raft) GetRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) Persist() {
	data := rf.GetRaftState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) ReadPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// This function:
// 1. Appends the raft information to the kv server snapshot
// 2. Ensures that the snapshot includes changes up to log entry with given index
// 3. Saves the data of the entire snapshot
//
func (rf *Raft) CreateSnapshot(kvSnapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex, lastIndex := rf.log[0].Index, rf.GetLastLogIndex()
	if index <= baseIndex || index > lastIndex {
		panic("Error: The index given is invalid, trimming log operation failed")
		return
	}
	rf.ShortenLog(index, rf.log[index-baseIndex].Term)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log[0].Index)
	e.Encode(rf.log[0].Term)
	snapshot := append(w.Bytes(), kvSnapshot...)

	rf.persister.SaveStateAndSnapshot(rf.GetRaftState(), snapshot)
}

//
// This function:
// Recover system based on data from previous raft snapshot
//
func (rf *Raft) RecoverFromSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var lastIncludedIndex, lastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	d.Decode(&lastIncludedIndex)
	d.Decode(&lastIncludedTerm)

	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.ShortenLog(lastIncludedIndex, lastIncludedTerm)

	// send snapshot to kv server
	msg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
	rf.chanApply <- msg
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// index of candidate’s last log entry
	LastLogIndex int
	// term of candidate’s last log entry
	LastLogTerm int
	// candidate’s term
	Term int
	// candidate requesting vote
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// currentTerm, for candidate to update itself
	Term int
	// true means candidate received vote
	VoteGranted bool
}

//
// example RequestVote RPC handler.
// RequestVote RPCs are initiated by candidates during elections
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Persist()

	if args.Term < rf.currentTerm {
		// Term number of candidate is not up to date
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// Candidate is valid, become follower
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// the RPC includes information about the candidate’s log, and the voter denies its vote if its own log is more
	// up-to-date than that of the candidate
	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.IsUpToDate(args.LastLogTerm, args.LastLogIndex) {
		// Vote for the candidate
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanGiveVote <- true
	}
}

//
// Function determines if the candidate's log is at least as new as the voter.
//
func (rf *Raft) IsUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.GetLastLogTerm(), rf.GetLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

func (rf *Raft) BroadcastRequestVote() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.GetLastLogIndex()
	args.LastLogTerm = rf.GetLastLogTerm()
	rf.mu.Unlock()

	for server := range rf.peers {
		if server != rf.me && rf.state == STATE_CANDIDATE {
			go rf.SendRequestVote(server, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Persist()

	if ok {
		if rf.state != STATE_CANDIDATE || rf.currentTerm != args.Term {
			// An invalid request was made
			return ok
		}
		if rf.currentTerm < reply.Term {
			// Change to follower state and update the current term
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return ok
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				// Determines majoity vote?  The election has been won
				rf.state = STATE_LEADER
				rf.Persist()
				// The leader maintains a nextIndex for each follower, which is the index of the
				// next log entry the leader will send to that follower. When a leader first comes
				// to power, it initializes all nextIndex values to the index just after the last
				// one in its log
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				nextIndex := rf.GetLastLogIndex() + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
				rf.chanWinElect <- true
			}
		}
	}

	return ok
}

//
// This function is initiated by leaders to replicate log entries and to provide a form of heartbeat
//  Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries) to all followers
// in order to maintain their authority
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.Persist()

	reply.Success = false

	//  Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		// This request is less up to date based on term number
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.GetLastLogIndex() + 1
		return
	}

	//	If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
	// then the candidate recognizes the leader as legitimate and returns to follower state
	if args.Term > rf.currentTerm {
		// Become follower, update current term
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// Checks the heartbeat, then refresh heartbeat timeout
	rf.chanHeartbeat <- true

	reply.Term = rf.currentTerm

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.GetLastLogIndex() {
		reply.NextTryIndex = rf.GetLastLogIndex() + 1
		return
	}

	baseIndex := rf.log[0].Index

	//  If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it

	// Should the log[prevLogIndex] conflict with new entry to be appended then conficting entries may have been
	// entered previously, in that case skip over all entries recorded during the conflicting term to speed up.
	if args.PrevLogIndex >= baseIndex && args.PrevLogTerm != rf.log[args.PrevLogIndex-baseIndex].Term {
		term := rf.log[args.PrevLogIndex-baseIndex].Term
		for i := args.PrevLogIndex - 1; i >= baseIndex; i-- {
			if rf.log[i-baseIndex].Term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else if args.PrevLogIndex >= baseIndex-1 {
		// Should the log[prevLogIndex] not conflict with new entry to be appended, this indicates the log records up
		// to prevLogIndex are valid.  Then combine the local log and entries sent from leader, finally commit log if
		// commitIndex changes.
		rf.log = rf.log[:args.PrevLogIndex-baseIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)

		if rf.commitIndex < args.LeaderCommit {
			// Apply update commitIndex and commit log
			// Append any new entries not already in the log if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
			rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogIndex())
			go rf.CommitLog()
		}
	}
	// AppendEntries returns successfully, the leader knows that the follower’s log
	// is identical to its own log up through the new entries
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != STATE_LEADER || args.Term != rf.currentTerm {
		// An invalid request was made
		return ok
	}
	if reply.Term > rf.currentTerm {
		// Ensure state is follower and then update the current term
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.Persist()
		return ok
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = min(reply.NextTryIndex, rf.GetLastLogIndex())
	}

	baseIndex := rf.log[0].Index
	for N := rf.GetLastLogIndex(); N > rf.commitIndex && rf.log[N-baseIndex].Term == rf.currentTerm; N-- {
		// Ensure commitIndex is up to date
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.CommitLog()
			break
		}
	}
	return ok
	// whenever AppendEntries returns successfully, the leader knows that the
	// follower’s log is identical to its own log up through the new entries
}

//
// Function commits log entries with index in range [lastApplied + 1, commitIndex]
//
func (rf *Raft) CommitLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandIndex = i
		msg.CommandValid = true
		msg.Command = rf.log[i-baseIndex].Command
		rf.chanApply <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, index := -1, -1
	isLeader := (rf.state == STATE_LEADER)

	if isLeader {
		term = rf.currentTerm
		index = rf.GetLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.Persist()
	}
	return index, term, isLeader
}

//
// Function in charge of broadcasting heartbeats to all followers
// This heartbeat may be accompanied by AppendEntries or InstallSnapshot
// depending on if the required log entry is eliminated or not.
//
func (rf *Raft) BroadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	snapshot := rf.persister.ReadSnapshot()

	for server := range rf.peers {
		if server != rf.me && rf.state == STATE_LEADER {
			if rf.nextIndex[server] > baseIndex {
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[server] - 1
				if args.PrevLogIndex >= baseIndex {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				}
				if rf.nextIndex[server] <= rf.GetLastLogIndex() {
					args.Entries = rf.log[rf.nextIndex[server]-baseIndex:]
				}
				args.LeaderCommit = rf.commitIndex

				go rf.SendAppendEntries(server, args, &AppendEntriesReply{})
			} else {
				args := &InitiateSnapshotArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.log[0].Index
				args.LastIncludedTerm = rf.log[0].Term
				args.Data = snapshot

				go rf.SendInstallSnapshot(server, args, &InitiateSnapshotReply{})
			}
		}
	}
}

// During election
func (rf *Raft) Operate() {
	for {
		switch rf.state {
		case STATE_LEADER:
			go rf.BroadcastHeartbeat()
			time.Sleep(time.Millisecond * 60)
		case STATE_CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.Persist()
			rf.mu.Unlock()
			go rf.BroadcastRequestVote()

			select {
			case <-rf.chanHeartbeat:
				rf.state = STATE_FOLLOWER
			case <-rf.chanWinElect:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
			}
		case STATE_FOLLOWER:
			select {
			case <-rf.chanGiveVote:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				rf.state = STATE_CANDIDATE
				rf.Persist()
			}
		}
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = STATE_FOLLOWER
	rf.voteCount = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanGiveVote = make(chan bool, 100)
	rf.chanWinElect = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanApply = applyCh

	// Initialize from persistent state prior to crash
	rf.ReadPersist(persister.ReadRaftState())
	rf.RecoverFromSnapshot(persister.ReadSnapshot())
	rf.Persist()

	go rf.Operate()

	return rf
}
