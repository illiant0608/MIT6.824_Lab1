package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
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

type PeerState string

const (
	Leader    PeerState = "LEADER"
	Follower  PeerState = "FOLLOWER"
	Candidate PeerState = "CANDIDATE"
	None      PeerState = "NONE"
)

const VoteNil = -1

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// todo log的内容具体是什么？
type LogEntry struct {
	Term    int
	Command interface{}
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
	currentTerm int
	voteFor     int
	log         []LogEntry
	applyCh     chan ApplyMsg

	electionTimeout       int
	electionTimeoutChan   chan bool
	heartbeatInterval     int
	heartbeatIntervalChan chan bool

	lastHeardTime     int64 // follower 上次收到AppendEntries时间
	lastHeartBeatTime int64 // leader 上次发送心跳时间

	// 条件变量
	nonLeaderCond *sync.Cond
	leaderCond    *sync.Cond

	// 节点状态
	state PeerState
}

// 不应该加锁，如果里面也加锁可能会导致死锁
func (rf *Raft) switchTo(newState PeerState) {
	oldState := rf.state
	rf.state = newState
	if oldState == Leader && newState == Follower {
		rf.nonLeaderCond.Broadcast()
	} else if oldState == Follower && newState == Leader {
		rf.leaderCond.Broadcast()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// 一个节点怎么判定自己是不是leader？
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateId int // candidate's id
	Term        int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Vote    bool // Vote or not
	VoterId int
	Term    int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
	Succ bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果当前任期没投过票，就投给请求节点
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Peer %d received vote request from Peer %d. CurrentTerm: %d, ArgsTerm: %d. VoteFor: %d",
		rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.voteFor)
	reply.Vote = false
	reply.Term = rf.currentTerm
	reply.VoterId = rf.me
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = VoteNil
		rf.switchTo(Follower)
	}

	// args.Term == rf.currentTerm
	if rf.voteFor == VoteNil || rf.voteFor == args.CandidateId {
		DPrintf("Voting to ...")
		reply.Vote = true
		rf.voteFor = args.CandidateId
		rf.switchTo(Follower)
		DPrintf("Peer %d vote for Peer %d, %v", rf.me, args.CandidateId, reply)
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Succ = false

	// 当前任期比心跳消息中的任期大，继续保持leader身份
	if rf.currentTerm > args.Term {
		return
	}

	rf.voteFor = VoteNil
	rf.switchTo(Follower)
	reply.Succ = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("Peer %d send vote request to Peer %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) resetElectionTimer() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rf.heartbeatInterval*5 + rand.Intn(150)
	rf.lastHeardTime = time.Now().UnixNano()
}

func (rf *Raft) Vote() {
	// 对rf.peers里的peer发送请求vote消息，发送消息用开goroutine处理
	// 然后等待reply，根据reply内容判断选举是否成功
	// 如果reply里面的term > currentTer，放弃选举，成为候选人
	// 超过半数，选举成功
	DPrintf("[ElectionTimeout] Id %d start to elect", rf.me)
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.voteFor = rf.me
	args := RequestVoteArgs{
		CandidateId: rf.me,
		Term:        rf.currentTerm,
	}
	rf.resetElectionTimer()
	rf.mu.Unlock()

	var wg sync.WaitGroup

	nVotes := 1 // 获得票数
	majority := len(rf.peers)/2 + 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		var reply RequestVoteReply
		go func(i int, rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
			defer wg.Done()
			ok := rf.sendRequestVote(i, args, reply)
			if ok == false {
				DPrintf("Send Vote Request Failed")
				return
			}

			if !reply.Vote {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("[Vote]: Peer %d Term %d Vote request rejected by Peer %d", rf.me, rf.currentTerm, i)

				if rf.currentTerm < reply.Term {
					DPrintf("[Vote]: Peer %d Term %d term is lower than Peer %d Term %d. Switch back to follower", rf.me, rf.currentTerm,
						i, reply.Term)
					rf.currentTerm = reply.Term
					rf.voteFor = VoteNil
					rf.switchTo(Follower)
				}
			} else {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("[Vote]: Peer %d Term %d Vote gets vote from Peer %d", rf.me, rf.currentTerm, i)
				nVotes++

				if rf.state == Candidate && nVotes >= majority {
					DPrintf("[Vote] Peer %d Term %d wins the election", rf.me, rf.currentTerm)
					rf.switchTo(Leader)

					// go 发送心跳
					go rf.broadcastHeartbeat()
				}
			}
		}(i, rf, &args, &reply)
	}
	wg.Wait()
}

func (rf *Raft) broadcastHeartbeat() {
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}
	rf.mu.Lock()
	rf.lastHeartBeatTime = time.Now().UnixNano()
	rf.mu.Unlock()

	rf.mu.Lock()
	//index := len(rf.log)-1
	//nReplica := 1
	go rf.broadcastAppendEntries(rf.currentTerm)
	rf.mu.Unlock()
}

func (rf *Raft) broadcastAppendEntries(term int) {
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)

		go func(i int, rf *Raft) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:     term,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply

			ok := rf.sendAppendEntries(i, &args, &reply)
			if !ok {
				DPrintf("Peer %d send broadcast append entries request failed.", args.LeaderId)
				return
			}

			if !reply.Succ {
				rf.mu.Lock()
				DPrintf("Peer %d append entries request is rejected by peer %d", rf.me, i)

				if args.Term < reply.Term {
					rf.currentTerm = reply.Term
					rf.voteFor = VoteNil
					rf.switchTo(Follower)
					rf.mu.Unlock()
					return
				} else {
					// todo
				}
			} else {
				//
				rf.mu.Lock()
				DPrintf("Peer %d append entries request to peer %d success", rf.me, i)
				rf.mu.Unlock()
			}
		}(i, rf)
	}
	wg.Wait()
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
// Term. the third return value is true if this server believes it is
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

func (rf *Raft) electionTimeoutTick() {
	for {
		if term, isLeader := rf.GetState(); isLeader {
			// 如果是leader，不需要参加选举，等待失去leader身份
			rf.mu.Lock()
			rf.nonLeaderCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			elapseTime := time.Now().UnixNano() - rf.lastHeardTime
			if int(elapseTime/int64(time.Millisecond)) >= rf.electionTimeout {
				DPrintf("[ElectionTimeoutTick]: Peer %d Term %d Timeout, Convert state to candidate.", rf.me, term)
				rf.electionTimeoutChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) heartbeatTick() {
	for {
		if term, isLeader := rf.GetState(); !isLeader {
			// 如果不是leader，不需要发送心跳，等待成为leader
			rf.mu.Lock()
			rf.leaderCond.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			elapseTime := time.Now().UnixNano() - rf.lastHeartBeatTime
			if int(elapseTime/int64(time.Millisecond)) >= rf.heartbeatInterval {
				DPrintf("[HeartbeatTimeoutTick]: Peer %d Term %d send heartbeat period elapsed.", rf.me, term)
				rf.heartbeatIntervalChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) eventLoop() {
	for {
		select {
		case <-rf.electionTimeoutChan:
			rf.mu.Lock()
			DPrintf("Peer %d Term %d election timeout, start an election.", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			go rf.Vote()
		case <-rf.heartbeatIntervalChan:
			rf.mu.Lock()
			DPrintf("Peer %d Term %d start to broadcast heartbeat.", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			// go rf....
		}
	}
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
	rf.applyCh = applyCh
	rf.state = Follower
	rf.currentTerm = 0
	rf.voteFor = VoteNil
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.nonLeaderCond = sync.NewCond(&rf.mu)
	rf.heartbeatInterval = 120

	rf.resetElectionTimer()
	rf.electionTimeoutChan = make(chan bool)
	rf.heartbeatIntervalChan = make(chan bool)

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		Term:    0,
	})

	DPrintf("Application Start. Peer Id: %d", rf.me)

	go rf.electionTimeoutTick()
	go rf.heartbeatTick()
	go rf.eventLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
