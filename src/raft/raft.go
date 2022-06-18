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
	"math"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Log struct {
	Term int
	Cmd  interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	applyCh         chan ApplyMsg
	currentTerm     int
	voteFor         int
	log             []Log
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	nVote           int
	role            Role
	heartBeatElapse time.Duration
	nCopyed         []int
	resetTimeout    int32
}
type Role int

const (
	FOLLOWER Role = iota + 1
	CANDIDATE
	LEADER
)

const NONE = -1

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (this *Raft) GetState() (int, bool) {

	var term int
	isleader := false
	this.mu.Lock()
	term = this.currentTerm
	if this.role == LEADER {
		isleader = true
	}
	this.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (this *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(this.xxx)
	// e.Encode(this.yyy)
	// data := w.Bytes()
	// this.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (this *Raft) readPersist(data []byte) {
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
	//   this.xxx = xxx
	//   this.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (this *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (this *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVote
// example RequestVote RPC handler.
//
func (this *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.Debug(dVote, "Recv vote req: %v", *args)
	if args.Term > this.currentTerm {
		this.Debug(dTerm, "Term: convert 2 follower")
		this.currentTerm = args.Term
		this.SwitchRoleTo(FOLLOWER)
	}

	if args.Term < this.currentTerm {
		this.Debug(dVote, "Term: return false")
		reply.VoteGranted = false
		reply.Term = this.currentTerm
		return
	}

	if this.voteFor == NONE || this.voteFor == args.CandidateId {
		if this.isUpdatedLog(args) {
			this.Debug(dVote, "Granted vote")
			this.voteFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = this.currentTerm
			this.ResetTimeout()
		} else {
			this.Debug(dVote, "Updated: return false")
			reply.VoteGranted = false
			reply.Term = this.currentTerm
		}
	} else {
		this.Debug(dVote, "VoteFor: return false")
		reply.VoteGranted = false
		reply.Term = this.currentTerm
	}
	return

}
func (this *Raft) isUpdatedLog(args *RequestVoteArgs) bool {
	//later term is more up-to-date, the same term, longer is more up-to-date
	myLastLogIndex := len(this.log) - 1

	myLastLogTerm := this.log[myLastLogIndex].Term

	if myLastLogTerm > args.LastLogTerm {
		return false
	}
	if myLastLogTerm == args.LastLogTerm {
		if myLastLogIndex > args.LastLogIndex {
			return false
		}
	}
	return true

}
func (this *Raft) BoardCastReqVote(toSend *RequestVoteArgs) {
	// 对于没有收到回复的srv 保持发送,直到成为leader 或者 follower
	for srv := range this.peers {
		if srv == this.me {
			continue
		}

		go func(srv int, toSend *RequestVoteArgs) {
			var reply = &RequestVoteReply{}
			for {
				this.mu.Lock()
				if toSend.Term != this.currentTerm || this.role != CANDIDATE {
					return
				}
				this.mu.Unlock()
				Dprintf(this.me, dVote, "Sendto %d, msg: %v", srv, toSend)

				ok := this.sendRequestVote(srv, toSend, reply)
				if ok {
					this.HandleVoteReply(reply)
					break
				}
				Dprintf(this.me, dVote, "fail to send srv%d", srv)
				time.Sleep(100 * time.Millisecond)
			}
		}(srv, toSend)
	}

}
func (this *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.Debug(dLog, "Recv apd req: %v", *args)
	if args.Term > this.currentTerm {
		this.Debug(dLog, "Term: convert 2 follower")
		this.currentTerm = args.Term
		this.SwitchRoleTo(FOLLOWER)
	}

	if this.role == CANDIDATE {
		this.SwitchRoleTo(FOLLOWER)
	}

	this.ResetTimeout()

	if args.Term < this.currentTerm {
		this.Debug(dLog, "Term: return false")
		reply.Success = false
		reply.Term = this.currentTerm
		return
	}

	if !this.matchPrevLog(args) {
		this.Debug(dLog, "!match: return false")
		reply.Success = false
		reply.Term = this.currentTerm
		return
	}
	this.Debug(dLog, "Before append:%v", this.log)
	if LogConflict(args, this.log) == true {
		this.Debug(dLog, "LogConflict")
		//如果是冲突那肯定是在这个范围以内
		this.log = this.log[:args.PrevLogIndex+1]
	}
	this.log = append(this.log, args.Entries...)
	this.Debug(dLog, "After append:%v", this.log)
	if args.LeaderCommit > this.commitIndex {
		this.commitIndex = Min(args.LeaderCommit, len(this.log)-1)
	}

	reply.Success = true
	reply.Term = this.currentTerm
	return

}
func (this *Raft) matchPrevLog(args *AppendEntriesArgs) bool {
	limitPrevIdx := len(this.log) - 1
	if limitPrevIdx <= 0 {
		return true
	}

	if args.PrevLogIndex > limitPrevIdx {
		// if log doesn't contain an entry at prevLogIndex whose term matches PrevLogTerm
		return false
	}

	if args.PrevLogTerm != this.log[args.PrevLogIndex].Term {
		return false
	}

	return true

}
func LogConflict(args *AppendEntriesArgs, myLogs []Log) bool {
	//TODO optimization: batching entries
	if args.PrevLogIndex+1 >= len(myLogs) {
		return false
	}
	if myLogs[args.PrevLogIndex+1].Term == args.Entries[0].Term {
		return false
	}
	return true
}

// Start
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
func (this *Raft) Start(command interface{}) (int, int, bool) {
	this.mu.Lock()
	defer this.mu.Unlock()
	index := len(this.log)
	term := this.currentTerm
	isLeader := this.role == LEADER
	newEntry := Log{
		Term: this.currentTerm,
		Cmd:  command,
	}

	//todo: optimazation: batching
	this.log = append(this.log, newEntry)
	return index, term, isLeader
}

// Kill
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
func (this *Raft) Kill() {
	atomic.StoreInt32(&this.dead, 1)
	// Your code here, if desired.
}

func (this *Raft) killed() bool {
	z := atomic.LoadInt32(&this.dead)
	return z == 1
}

func (this *Raft) ResetTimeout() {
	//Dprintf(this.me, dTimer, "ResetTimeout")
	atomic.SwapInt32(&this.resetTimeout, 1)
}

func (this *Raft) reSleep() bool {
	z := atomic.LoadInt32(&this.resetTimeout)
	if z == 1 {
		atomic.SwapInt32(&this.resetTimeout, 0)
		return true
	} else {
		return false
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (this *Raft) ticker() {
	sleepTicks := math.MinInt32
	for this.killed() == false {
		debugTime := 0
		rand.Seed(time.Now().UnixNano())

		sleepTicks = rand.Intn(7) + 8

		for sleepTicks != 0 {
			switch this.reSleep() {
			case true:
				Dprintf(this.me, dTimer, "Resleep")
				sleepTicks = rand.Intn(7) + 8
			default:
				sleepTicks--
			}
			debugTime += 40
			time.Sleep(40 * time.Millisecond)
		}

		this.mu.Lock()
		if this.role == CANDIDATE || this.role == FOLLOWER {
			this.Debug(dTimer, "start election timeout: %d ms", debugTime)
			this.SwitchRoleTo(CANDIDATE)
		}
		this.mu.Unlock()

	}
}
func (this *Raft) StartElection() {
	this.mu.Lock()

	args := &RequestVoteArgs{
		Term:         this.currentTerm,
		CandidateId:  this.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	this.mu.Unlock()
	go this.BoardCastReqVote(args)
}

func (this *Raft) HandleVoteReply(vote *RequestVoteReply) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.Debug(dVote, "Recv vote reply")
	term := vote.Term
	if term > this.currentTerm {
		this.currentTerm = term
		this.SwitchRoleTo(FOLLOWER)
		return
	}

	if this.role != CANDIDATE {
		this.Debug(dWarn, "Not candidate: return")
		return
	}

	if vote.VoteGranted == true {
		this.nVote++
	}

	if this.nVote >= len(this.peers)/2+1 {
		this.SwitchRoleTo(LEADER)
	}

}

//持锁调用
func (this *Raft) SwitchRoleTo(role Role) {
	if role == this.role {
		return
	}

	switch role {
	case FOLLOWER:
		this.Debug(dTrace, "Convert 2 FOLLOWER")
		this.nVote = 0
		this.voteFor = NONE
		this.role = FOLLOWER

		this.ResetTimeout()
	case CANDIDATE:
		this.Debug(dTrace, "Convert 2 CANDIDATE")
		this.currentTerm++
		this.voteFor = this.me
		this.nVote = 0
		this.nVote++
		this.role = CANDIDATE

		this.ResetTimeout()
		go this.StartElection()
	case LEADER:
		this.Debug(dTrace, "Convert 2 LEADER")
		this.role = LEADER
		//初始化 leader
		for i := range this.nextIndex {
			this.nextIndex[i] = len(this.log)
		}
		for i := range this.matchIndex {
			this.matchIndex[i] = 0
		}
		for i := range this.nCopyed {
			this.nCopyed[i] = 0
		}
		this.Debug(dLeader, "LeaderInit: nextIndex:%v, matchIndex:%v, nCopyed:%v", this.nextIndex, this.matchIndex, this.nCopyed)
		go this.StartLeader()
	}
}

// HandleApdEntriesReply
//todo handle the apd entries reply
func (this *Raft) HandleApdEntriesReply(aEntry *AppendEntriesReply, srv int, send *AppendEntriesArgs) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if aEntry.Term > this.currentTerm {
		this.Debug(dTerm, "Convert 2 Follower")
		this.currentTerm = aEntry.Term
		this.SwitchRoleTo(FOLLOWER)
		return
	}

	if this.role != LEADER {
		this.Debug(dVote, "Not a leader: return")
		return
	}

	if aEntry.Success == true {
		this.nextIndex[srv] += len(send.Entries)
		this.matchIndex[srv] = send.PrevLogIndex + len(send.Entries)
		this.Debug(dLog, "Success reply After: mtchIdx: %d, nxtIdx:%d", this.matchIndex[srv], this.nextIndex[srv])
		this.UpdateCommit(srv)
	} else {
		// 返回失败就两种原因,第一种就是因为term , 第二种就是因为prev 不匹配
		//todo optimzation: backtraking nextIndex
		this.nextIndex[srv]--
	}
}
func (this *Raft) UpdateCommit(srv int) {
	//nCopyed 只要比 nextIndex 大就行
	n := len(this.nCopyed)

	if this.nextIndex[srv] > n-1 {
		incLen := this.matchIndex[srv] - len(this.nCopyed) + 1
		this.nCopyed = append(this.nCopyed, make([]int, incLen)...)
	} else {
		this.nCopyed[this.matchIndex[srv]]++
		this.Debug(dCommit, "matchIdx: %d, loglen:%d", this.matchIndex[srv], len(this.log))
		if this.nCopyed[this.matchIndex[srv]] >= len(this.peers)/2+1 &&
			this.log[this.matchIndex[srv]].Term == this.currentTerm {
			//todo 不太确定要不要用max
			this.Debug(dCommit, "cmitIdx: %d", this.commitIndex)
			this.commitIndex = Max(this.commitIndex, this.matchIndex[srv])
		}
	}
	if this.lastApplied < this.commitIndex {
		this.Applier()
	}

}
func (this *Raft) Applier() {

	toApplys := this.log[this.lastApplied+1 : this.commitIndex+1]

	for i, toApply := range toApplys {

		var msg ApplyMsg
		msg.CommandValid = true
		msg.Command = toApply.Cmd
		msg.CommandIndex = this.lastApplied + i + 1

		this.applyCh <- msg

		if this.lastApplied < msg.CommandIndex {
			this.lastApplied = msg.CommandIndex
		}

	}
}
func (this *Raft) BoardCastApdEntries() {
	for srv := range this.peers {
		if srv == this.me {
			continue
		}

		go func(srv int) {
			var toSend *AppendEntriesArgs
			var entries []Log
			for {
				this.mu.Lock()
				if this.role != LEADER {
					this.Debug(dLeader, "Not Leader, stop sending apd entries")
					this.mu.Unlock()
					return
				}

				if this.BeHeartBeat(srv) == true {
					entries = nil
				} else {
					//todo optimization: batching logs
					this.Debug(dLeader, "Have entry: nxtIdx: %d, mtchIdx: %d", this.nextIndex[srv], this.matchIndex[srv])
					entries = make([]Log, 0)
					entries = append(entries, this.log[this.nextIndex[srv]])
				}
				prevLogTerm := 0
				if this.nextIndex[srv] <= len(this.log)-1 {
					prevLogTerm = this.log[this.nextIndex[srv]-1].Term
				}
				toSend = &AppendEntriesArgs{
					Term:         this.currentTerm,
					LeaderId:     this.me,
					PrevLogIndex: this.nextIndex[srv] - 1,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: this.commitIndex,
				}
				reply := &AppendEntriesReply{}
				this.Debug(dLeader, "sendTo:%d, msg:%v", srv, toSend)
				this.mu.Unlock()

				ok := this.sendAppendEntries(srv, toSend, reply)

				if ok {
					//todo:[refactor]: ticket pool when apd entries
					this.HandleApdEntriesReply(reply, srv, toSend)
					break
				}
				Dprintf(this.me, dLeader, "Fail to send %d", srv)
			}
		}(srv)
	}
}
func (this *Raft) BeHeartBeat(srv int) bool {
	if this.nextIndex[srv] >= len(this.log) {
		return true
	}
	//todo 这个优化动画上有实现,注意实现正确性
	if this.nextIndex[srv] != this.matchIndex[srv]+1 {
		return true
	}
	return false
}

func (this *Raft) StartLeader() {
	for this.killed() == false {
		this.mu.Lock()
		if this.role != LEADER {
			this.mu.Unlock()
			return
		}
		this.mu.Unlock()
		go this.BoardCastApdEntries()
		time.Sleep(this.heartBeatElapse)
	}
}

// Make
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
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.mu = sync.Mutex{}
	rf.dead = 0
	rf.currentTerm = 1
	rf.voteFor = NONE
	//dummy node
	rf.log = make([]Log, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.nVote = 0
	rf.role = FOLLOWER
	rf.heartBeatElapse = 100 * time.Millisecond
	rf.nCopyed = make([]int, 20)
	rf.resetTimeout = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
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
// within a reSleep interval, Call() returns true; otherwise
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
