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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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

// time
const (
	HeartBeatTimeout = time.Millisecond * 100 // leader 发送心跳
	ApplyInterval    = time.Millisecond * 100 // apply log
	RPCTimeout       = time.Millisecond * 100
	MaxLockTime      = time.Millisecond * 10 // debug
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
	voteFor int
	


	// 2A
	state         State
	currentTerm   int
	votedFor      int
	timeout       time.Duration
	connectTime   time.Time // this peer's last connect time with leader or start time of new election
	heartbeatTime time.Time
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
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	Term        int
	CandidateId int
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
}

type AppendEntriesReply struct {
	// 2A
	Term    int
	Success bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) callRequestVote(server int, term int) bool {
	DPrintf("[%d] <%s> send Request Vote to %d at term %d", rf.me, rf.state, server, term)
	// initial args and reply
	args := RequestVoteArgs{
		Term:        term,
		CandidateId: rf.me,
	}
	reply := RequestVoteReply{}
	// send rpc
	if ok := rf.sendRequestVote(server, &args, &reply); !ok {
		DPrintf("[%d] <%s> failed to sendRequestVote to %d at term %d", rf.me, rf.state, server, term)
		return false // rpc failed，VoteGranted = false
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

	// args.Term >= rf.currentTerm
	// reset election timeout
	rf.connectTime = time.Now()

	if args.Term > rf.currentTerm {
		// update rf.currentTerm
		rf.currentTerm = args.Term
		// state -> Follower, reset votedFor, and vote
		//if rf.state != Follower {
		DPrintf("[%d] <%s> RequestVote term -> %d state->Follower", rf.me, rf.state,rf.currentTerm)
		rf.state = Follower
		rf.votedFor = -1
		//}
	}

	reply.Term = rf.currentTerm
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		DPrintf("[%d] <%s> RequestVote vote for %d at term %d", rf.me, rf.state, rf.votedFor, rf.currentTerm)
		reply.VoteGranted = true
	} else if rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
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
	//DPrintf("[%d] <%s> callAppendEntries to %d at term %d", rf.me, rf.state, server, term)
	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
	}
	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
		//DPrintf("[%d] <%s> failed to callAppendEntries to %d at term %d", rf.me, rf.state, server, term)
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
			rf.persist()
			return false
		}
	}
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("[%d] <%s> AppendEntries at term %d", rf.me, rf.state, rf.currentTerm)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf("[%d] <%s> AppendEntries from old leader at term %d", rf.me, rf.state, rf.currentTerm)
		reply.Success = false
		return
	}
	// reset election timeout
	rf.connectTime = time.Now()
	// args.Term >= rf.currentTerm
	// update rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		DPrintf("[%d] <%s> AppendEntries term -> %d", rf.me, rf.state, rf.currentTerm)
		// state -> Follower
		if rf.state != Follower {
			DPrintf("[%d] <%s> AppendEntries state -> Follower", rf.me, rf.state)
			rf.state = Follower
			rf.votedFor = -1
		}
	}

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

	// Your initialization code here (2A, 2B, 2C).

	// 2A
	go rf.ElectionRoutine()
	go rf.HeartbeatRoutine()
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
		rf.state = Leader
		rf.votedFor = -1
		rf.connectTime = time.Now() // reset election timer

		// reset heartbeat timer
		rf.heartbeatTime = time.Now() // reset heartbeat timer
		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				DPrintf("[%d] <%s> establish its authority to %d at term %d", rf.me, rf.state, server, rf.currentTerm)
				if ok := rf.callAppendEntries(server, rf.currentTerm); !ok {
					DPrintf("[%d] <%s> failed to establish its authority to %d at term %d", rf.me, rf.state, server, term)
				}
			}(server)
		}
	} else {
		// peer doesn't win the election
		DPrintf("[%d] <%s> lose the election at term %d, state -> Follower", rf.me, rf.state, rf.currentTerm)
		rf.state = Follower
		rf.votedFor = -1
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
							// 该peer在上半部分代码执行时为leader，但是在rpc超时后，peer的状态已经发生改变，造成了不是leader但heartbeat发送失败的假象
							//DPrintf("[%d] <%s> old state: <%s> failed to send heartbeat to %d at term %d", rf.me, rf.state, state, server, term)
						}
					}(server)
				}

			}

		}
	}
}
