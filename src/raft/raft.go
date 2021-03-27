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
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

// import "bytes"
// import "../labgob"

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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// peer's State
type State string

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

// Log
type LogEntry struct {
	Command interface{}
	Term    int
}

// time
const (
	HeartBeatTimeout = time.Millisecond * 100 // leader 发送心跳
	ApplyTimeout     = time.Millisecond * 100 // apply log
)

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
	currentItem int
	voteFor     int

	// 2A
	state         State
	currentTerm   int
	votedFor      int
	leaderId      int
	timeout       time.Duration
	connectTime   time.Time // this peer's last connect time with leader or start time of new election
	heartbeatTime time.Time

	// 2B
	log []LogEntry
	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg

	// 2C
	lastIncludedIndex int
	lastIncludedTerm  int
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// 2C
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)

	DPrintf("[%d] <%s> persist state currentTerm %d votedFor %d len(log) %d to stable storage at term %d", rf.me, rf.state, rf.currentTerm,
		rf.votedFor, len(rf.log), rf.currentTerm)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	// 2C
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if decoder.Decode(&currentTerm) != nil || decoder.Decode(&votedFor) != nil || decoder.Decode(&log) != nil {
		DPrintf("[%d] <%s> persist state from stable storage failed at term %d", rf.me, rf.state, rf.currentTerm)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
	//decoder.Decode(&rf.currentTerm)
	//decoder.Decode(&rf.votedFor)
	//decoder.Decode(&rf.log)

	DPrintf("[%d] <%s> persist state from stable storage success term -> %d votedFor -> %d len(log) -> %d", rf.me, rf.state, rf.currentTerm, rf.votedFor, len(rf.log))

}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	Term        int
	CandidateId int

	// 2B
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).

	// 2A
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// 2A
	Term     int
	LeaderId int

	// 2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// 2A
	Term    int
	Success bool
	//conflictingEntryItem int
	ConflictingFirstEntryIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) callRequestVote(server int, term int) bool {
	DPrintf("[%d] <%s> send Request Vote to %d at term %d", rf.me, rf.state, server, term)
	// initial args and reply
	lastLogIndex := 0
	lastLogTerm := 0
	if rf.lastLogIndex() > 0 {
		lastLogIndex = rf.lastLogIndex()
		lastLogTerm = rf.log[rf.lastLogIndex()-1].Term
	}
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	reply := RequestVoteReply{}
	// send rpc
	if ok := rf.sendRequestVote(server, &args, &reply); !ok {
		DPrintf("[%d] <%s> failed to sendRequestVote to %d at term %d", rf.me, rf.state, server, term)
		rf.persist()
		return false // rpc failed
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		DPrintf("[%d] <%s> sendRequestVote is old, term-> %d, state->Follower", rf.me, rf.state, rf.currentTerm)
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		return false
	}
	rf.persist()
	return reply.VoteGranted

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] <%s> RequestVote at term %d", rf.me, rf.state, rf.currentTerm)

	// old request
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// update rf.currentTerm
		rf.currentTerm = args.Term
		// state -> Follower, reset votedFor, and vote
		DPrintf("[%d] <%s> RequestVote term -> %d state->Follower", rf.me, rf.state, rf.currentTerm)
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		rf.connectTime = time.Now()
	}

	reply.Term = rf.currentTerm

	isUpToDate := false
	lastLogTerm := -1 // 为了保证第一次能够成功
	if rf.lastLogIndex() > 0 {
		lastLogTerm = rf.log[rf.lastLogIndex()-1].Term
	}
	DPrintf("[%d] <%s> RequestVote args.LastLogIndex: %d args.LastLogTerm: %d lastLogIndex: %d "+
		"lastLogTerm: %d at term %d", rf.me, rf.state, args.LastLogIndex, args.LastLogTerm, rf.lastLogIndex(),
		lastLogTerm, rf.currentTerm)

	if args.LastLogTerm > lastLogTerm {
		isUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastLogIndex() {
		isUpToDate = true
	}

	if rf.votedFor == -1 && isUpToDate {
		rf.votedFor = args.CandidateId
		DPrintf("[%d] <%s> RequestVote vote for %d at term %d", rf.me, rf.state, rf.votedFor, rf.currentTerm)
		reply.VoteGranted = true
		rf.connectTime = time.Now()
	} else if rf.votedFor == args.CandidateId && isUpToDate {
		reply.VoteGranted = true
		rf.connectTime = time.Now()
	} else {
		DPrintf("[%d] <%s> RequestVote already vote for %d at term %d", rf.me, rf.state, rf.votedFor, rf.currentTerm)
		reply.VoteGranted = false
	}

	rf.persist()
	return

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) callAppendEntries(server int, term int) bool {
	// 2A
	DPrintf("[%d] <%s> lastLogIndex: %d commitIndex: %d lastApplied: %d callAppendEntries to [%d nextIndex: %d matchIndex: %d] at term %d",
		rf.me, rf.state, rf.lastLogIndex(), rf.commitIndex, rf.lastApplied, server, rf.nextIndex[server], rf.matchIndex[server], term)

	nextIndex := rf.nextIndex[server]
	prevLogIndex := nextIndex - 1
	prevLogTerm := 0 // 无Log时，prevLogTerm=0
	if prevLogIndex-1 >= 0 {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}

	entries := make([]LogEntry, 0)

	// last log index >= rf.nextIndex[server] (nextIndex for a follower)
	if rf.lastLogIndex() >= nextIndex {
		entries = append(entries, rf.log[nextIndex-1:]...)
		DPrintf("[%d] <%s> lastLogIndex: %d commitIndex: %d lastApplied: %d callAppendEntries to %d entriesNum: -%d- at term %d",
			rf.me, rf.state, rf.lastLogIndex(), rf.commitIndex, rf.lastApplied, server, len(entries), term)
	}

	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
		// 2B
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}
	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
		DPrintf("[%d] <%s> failed to callAppendEntries to %d at term %d", rf.me, rf.state, server, term)
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !reply.Success {
		// term is old
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			DPrintf("[%d] <%s> callAppendEntries to %d, old leader, term -> %d, state -> Follower", rf.me, rf.state, server, rf.currentTerm)
			rf.state = Follower
			rf.votedFor = -1
			rf.leaderId = -1
			rf.persist()
			return false
		}

		if reply.ConflictingFirstEntryIndex != 0 {
			rf.nextIndex[server] = reply.ConflictingFirstEntryIndex
		} else {
			rf.nextIndex[server]--
		}

		DPrintf("[%d] <%s> callAppendEntries reply=false to [%d nextIndex->%d matchIndex->%d] at term %d",
			rf.me, rf.state, server, rf.nextIndex[server], rf.matchIndex[server], rf.currentTerm)

		return false
	}
	// success
	DPrintf("[%d] <%s> callAppendEntries reply=success to [%d nextIndex: %d matchIndex: %d] at term %d",
		rf.me, rf.state, server, rf.nextIndex[server], rf.matchIndex[server], rf.currentTerm)

	rf.nextIndex[server] = nextIndex + len(entries)
	//rf.matchIndex[server] = rf.nextIndex[server] - 1 (student guide)
	rf.matchIndex[server] = prevLogIndex + len(args.Entries)
	// 自己的matchIndex没有修改
	// 所以count默认为1
	// 因为leader肯定是match的
	isUpdateCommitIndex := false
	var n int
	for n = rf.lastLogIndex(); n > rf.commitIndex; n-- {
		//count := 0
		count := 1
		for server, _ := range rf.peers {
			if rf.matchIndex[server] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			if rf.log[n-1].Term == rf.currentTerm {
				isUpdateCommitIndex = true
				break
			}
		}
	}
	if isUpdateCommitIndex {
		rf.commitIndex = n
		DPrintf("[%d] <%s> callAppendEntries commitIndex-> %d at term %d",
			rf.me, rf.state, rf.commitIndex, rf.currentTerm)
	}

	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] <%s> AppendEntries PrevlogIndex: %d PrevlogTerm: %d at term %d",
		rf.me, rf.state, args.PrevLogIndex, args.PrevLogTerm, rf.currentTerm)

	// old leader
	if args.Term < rf.currentTerm {
		DPrintf("[%d] <%s> AppendEntries from old leader at term %d", rf.me, rf.state, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// reset election timeout
	rf.connectTime = time.Now()
	rf.leaderId = args.LeaderId

	// update rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		DPrintf("[%d] <%s> AppendEntries term -> %d", rf.me, rf.state, rf.currentTerm)
		// state -> Follower
		if rf.state != Follower {
			DPrintf("[%d] <%s> AppendEntries state -> Follower", rf.me, rf.state)
			rf.state = Follower
			rf.votedFor = -1
			//rf.persist()
		}
		rf.persist()
	}
	reply.Term = rf.currentTerm

	DPrintf("[%d] AppendEntries PrevLogIndex %d lastLogIndex %d at term %d", rf.me, args.PrevLogIndex, rf.lastLogIndex(), rf.currentTerm)
	if args.PrevLogIndex > rf.lastLogIndex() {
		DPrintf("[%d] AppendEntries PrevLogIndex %d > lastLogIndex %d, Success=false at term %d", rf.me, args.PrevLogIndex, rf.lastLogIndex(), rf.currentTerm)
		reply.ConflictingFirstEntryIndex = rf.lastLogIndex()
		reply.Success = false
		return
	}

	// args.PrevLogIndex>=1保证访问不存在日志
	if args.PrevLogIndex <= rf.lastLogIndex() && args.PrevLogIndex >= 1 {
		// If an existing entry conflicts with a new one (same index but different terms)
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			DPrintf("[%d] <%s> AppendEntries PrevlogIndex: %d PrevlogTerm: %d doesnt match local entry index: %d term: %d, at term %d",
				rf.me, rf.state, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, rf.log[args.PrevLogIndex-1].Term, rf.currentTerm)
			//for conflictingFirstEntryIndex := rf.lastApplied + 1; conflictingFirstEntryIndex <= rf.lastLogIndex(); conflictingFirstEntryIndex++ {
			for conflictingFirstEntryIndex := 1; conflictingFirstEntryIndex <= rf.lastLogIndex(); conflictingFirstEntryIndex++ {
				if rf.log[conflictingFirstEntryIndex-1].Term == rf.log[args.PrevLogIndex-1].Term {
					reply.ConflictingFirstEntryIndex = conflictingFirstEntryIndex
					//DPrintf("[%d] <%s> AppendEntries firstIndex-> %d at term %d",
					//	rf.me, rf.state, reply.ConflictingFirstEntryIndex, rf.currentTerm)
					break
				}
			}
			reply.Success = false
			return
		}
	}

	//for i, entry := range args.Entries {
	//	if len(rf.log) > 0 {
	//		//DPrintf("index:%d term:%d command:%d", len(rf.log)-1, rf.log[len(rf.log)-1].Term, rf.log[len(rf.log)-1].Term)
	//	}
	//
	//	s := ""
	//	for _,log := range rf.log{
	//		s += fmt.Sprintf("%v ",log.Command)
	//	}
	//	DPrintf("[%d] <%s> appendEntries cmd {%s}",rf.me,rf.state,s)
	//
	//	// 该entry的实际index要比prevlogindex大1
	//	newEntryIndex := args.PrevLogIndex + i + 1
	//
	//	if newEntryIndex > rf.lastLogIndex() {
	//		rf.log = append(rf.log, entry)
	//		DPrintf("[%d] appendEntries {newEntryIndex > rf.lastLogIndex()} newEntry index: %d cmd: %v at term %d",
	//			rf.me, newEntryIndex, entry.Command, rf.currentTerm)
	//		continue
	//	}
	//	// 这里是地址，index-1
	//	if rf.log[newEntryIndex-1].Term != entry.Term {
	//		// delete the existing entry and all that follow it
	//		rf.log = rf.log[:newEntryIndex-1]
	//		rf.log = append(rf.log, args.Entries[i:]...)
	//		DPrintf("[%d] appendEntries conflict",rf.me)
	//		break
	//	} else if rf.log[newEntryIndex-1].Term == entry.Term {
	//		// aleady in the log
	//		continue
	//	}
	//}

	for i,entry := range args.Entries{
		newEntryIndex := args.PrevLogIndex + i + 1
		if newEntryIndex > rf.lastLogIndex(){
			rf.log = append(rf.log,entry)
			DPrintf("[%d] appendEntries {newEntryIndex > rf.lastLogIndex()} newEntry index: %d cmd: %v at term %d",
				rf.me, newEntryIndex, entry.Command, rf.currentTerm)
		}else if rf.log[newEntryIndex - 1].Term != entry.Term{
			rf.log = rf.log[:newEntryIndex-1]
			rf.log = append(rf.log,entry)
			DPrintf("[%d] appendEntries {entry term doesnt match} newEntry index: %d cmd: %v at term %d",
				rf.me, newEntryIndex, entry.Command, rf.currentTerm)
		}
	}


	//s := ""
	//for _,log := range rf.log{
	//	s += fmt.Sprintf("%v ",log.Command)
	//}
	//DPrintf("[%d] <%s> appendEntries finished cmd {%s}",rf.me,rf.state,s)

	//DPrintf("[%d] <%s> AppendEntries args.LeaderCommit %d rf.commitIndex %d rf.lastApplied %d", rf.me, rf.state, args.LeaderCommit, rf.commitIndex, rf.lastApplied)

	if args.LeaderCommit > rf.commitIndex {

		if rf.lastLogIndex() < args.LeaderCommit {
			rf.commitIndex = rf.lastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DPrintf("[%d] <%s> AppendEntries commitIndex-> %d at term %d",
			rf.me, rf.state, rf.commitIndex, rf.currentTerm)
	}

	DPrintf("[%d] <%s> AppendEntries have %d log at term %d", rf.me, rf.state, len(rf.log), rf.currentTerm)

	reply.Success = true
	rf.persist()
	return

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// 2B
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	// append log
	newLogEntry := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, newLogEntry)
	DPrintf("[%d] <%s> commitIndex: %d lastApplied: %d get %dst log at term %d",
		rf.me, rf.state, rf.commitIndex, rf.lastApplied, rf.lastLogIndex(), rf.currentTerm)
	index = rf.lastLogIndex()
	term = rf.currentTerm
	// Leader收到Entry，需要持久化保存
	rf.persist()
	return index, term, isLeader
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

	// 2A
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.timeout = time.Duration(300+rand.Int31n(200)) * time.Millisecond
	rf.connectTime = time.Now() // 判断是否超时以提交选举
	rf.heartbeatTime = time.Now()

	// 2B
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	// first initialize at election
	//rf.nextIndex = make([]int, len(rf.peers))
	//rf.matchIndex = make([]int, len(rf.peers))

	// Your initialization code here (2A, 2B, 2C).

	// 2A
	go rf.ElectionRoutine()
	go rf.HeartbeatRoutine()
	go rf.ApplyLogRoutine()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) ElectionRoutine() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		if time.Now().Sub(rf.connectTime) > rf.timeout {
			if rf.state == Candidate || rf.state == Follower {
				// start election
				//DPrintf("[%d] <%s> start election at term %d", rf.me, rf.state, rf.currentTerm)
				rf.startElection()
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++       // increase currentTerm
	term := rf.currentTerm // local currentTerm
	rf.state = Candidate   //
	rf.votedFor = rf.me    // vote for itself
	rf.timeout = time.Duration(300+rand.Int31n(200)) * time.Millisecond
	rf.connectTime = time.Now() // reset election timer
	rf.persist()                // updated on stable storage
	rf.mu.Unlock()
	DPrintf("[%d] <%s> start election at term -%d-", rf.me, rf.state, rf.currentTerm)
	// 统计获得的投票
	votes := 1
	finished := 1
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	// 发送投票请求
	for server, _ := range rf.peers {
		// skip this peer itself
		if server == rf.me {
			continue
		}
		// send RPC
		go func(server int) {
			VoteGranted := rf.callRequestVote(server, term) //在其中可能会对rf.currentTerm进行更改

			mu.Lock()
			defer mu.Unlock()

			// don't get the vote
			if !VoteGranted {
				DPrintf("[%d] <%s> didn't get a vote from %d at term %d", rf.me, rf.state, server, term)
				finished++
				cond.Broadcast()

				return
			}

			// count the votes
			DPrintf("[%d] <%s> got vote %d -> %d at term %d", rf.me, rf.state, server, rf.me, term)
			votes++
			finished++
			cond.Broadcast()

		}(server)
	}

	// wait for vote results
	mu.Lock()

	// until get majority votes( votes >= len(rf.peers)/2 ) or all RPC finished
	for votes <= len(rf.peers)/2 && finished != len(rf.peers) {
		cond.Wait()
	}

	DPrintf("[%d] <%s> finished: %d", rf.me, rf.state, finished)

	rf.mu.Lock() // 保证后面的操作中rf的各个变量都没有发生变化
	defer rf.mu.Unlock()

	if rf.currentTerm > term {
		rf.state = Follower
		rf.votedFor = -1
		rf.leaderId = -1
		rf.persist()
		return
	}
	// state change
	if rf.state != Candidate {
		return
	}

	// get majority votes
	if votes > len(rf.peers)/2 {
		DPrintf("[%d] <%s> got %d votes and became Leader at term %d", rf.me, rf.state, votes, rf.currentTerm)
		// state -> Leader
		// 2A
		rf.state = Leader
		rf.votedFor = -1
		rf.leaderId = rf.me
		rf.persist()
		rf.connectTime = time.Now()   // reset election timer
		rf.heartbeatTime = time.Now() // reset heartbeat timer

		// 2B
		// reinitialized after election
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for server, _ := range rf.peers {
			rf.nextIndex[server] = rf.lastLogIndex() + 1
			rf.matchIndex[server] = 0
		}

		rf.heartbeatTime = time.Now()
		//for server, _ := range rf.peers {
		//	if server == rf.me {
		//		continue
		//	}
		//	go func(server int) {
		//		DPrintf("[%d] <%s> establish its authority to %d at term %d", rf.me, rf.state, server, rf.currentTerm)
		//		if ok := rf.callAppendEntries(server, rf.currentTerm); !ok {
		//			DPrintf("[%d] <%s> failed to establish its authority to %d at term %d", rf.me, rf.state, server, term)
		//		}
		//	}(server)
		//}
	} else {
		// peer doesn't win the election
		DPrintf("[%d] <%s> lose the election at term %d, state -> Follower", rf.me, rf.state, rf.currentTerm)
		rf.state = Follower
		rf.votedFor = -1
		rf.leaderId = -1
		rf.persist()
	}

	mu.Unlock()

}

func (rf *Raft) HeartbeatRoutine() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		if _, isleader := rf.GetState(); isleader {
			// heartbeat timeout
			if time.Now().Sub(rf.heartbeatTime) > HeartBeatTimeout {
				// send heartbeat
				rf.mu.Lock()
				term := rf.currentTerm
				//state := rf.state
				rf.heartbeatTime = time.Now()
				rf.mu.Unlock()
				for server, _ := range rf.peers {
					if server == rf.me {
						continue
					}
					go func(server int) {
						// 再次判断是否是Leader
						if _, isleader := rf.GetState(); !isleader {
							return
						}
						if ok := rf.callAppendEntries(server, term); !ok {
							//if rf.commitIndex > rf.lastApplied {
							//	rf.lastApplied++
							//}
							// 该peer在上半部分代码执行时为leader，但是在rpc超时后，peer的状态已经发生改变，造成了不是leader但heartbeat发送失败的假象
							//DPrintf("[%d] <%s> old state: <%s> failed to send heartbeat to %d at term %d", rf.me, rf.state, state, server, term)
						}
					}(server)
				}
				// 记录各个peer回答的term

			}

		}
	}
}

func (rf *Raft) ApplyLogRoutine() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		//rf.mu.Lock()
		//defer rf.mu.Unlock()
		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
		for rf.commitIndex > rf.lastApplied {
			//s := ""
			//for _,log := range rf.log{
			//	s += fmt.Sprintf("%v ",log.Command)
			//}
			//DPrintf("[%d] <%s> ApplyLogRoutine cmd {%s}",rf.me,rf.state,s)
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-1].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- applyMsg
			DPrintf("[%d] <%s> ApplyLogRoutine len(log) %d applied %dst log commitIndex: %d lastApplied-> %d cmd: %v at term %d",
				rf.me, rf.state, len(rf.log),rf.lastApplied, rf.commitIndex, rf.lastApplied, rf.log[rf.lastApplied-1].Command, rf.currentTerm)
		}

	}
}
