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
	"../labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

var Leader int = 30
var Candidate int = 20
var Follower int = 10

var STARTTERM int = 0
var VOTENULL int = -1

var HEARTBEATTIMEOUT = 100

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

type LogEntry struct {
	LogIndex   int
	LogTerm    int
	LogCommand interface{}
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
	votedFor    int // ??????????????????????????????CandidateID???????????????????????????Candidate?????????None
	log         []LogEntry

	commitIndex int // ???????????????????????????????????????????????????????????????0??????????????????
	lastApplied int // ????????????????????????????????????????????????????????????????????????0??????????????????

	nextIndex []int // ?????????????????????????????????????????????????????????????????????????????????????????????????????????
	// ??????????????????????????????+1???
	matchIndex []int // ????????????????????????????????????????????????????????????????????????????????????????????????????????????0???
	// ????????????

	state     int // ????????????????????????Follower???Candidate???Leader
	leader    int // ?????????????????????????????????Leader
	voteCount int // ???????????????????????????

	appendEntriesCh chan bool // ?????????????????????Leader???AppendEntriesRPC?????????????????????true
	voteGrantedCh   chan bool // ?????????????????????Candidate???RequestVoteRPC????????????Candidate???????????????????????????true
	leaderCh        chan bool // ???????????????????????????Leader?????????????????????true
	commitCh        chan bool // ??????Leader?????????????????????????????????????????????????????????????????????true
	// ??????Follower???????????????leader.CommitIndex???????????????CommitIndex?????????????????????CommitIndex??????????????????true
	applyMsgCh chan ApplyMsg // ???????????????Log?????????state machine??????????????????????????????

	heartbeatTimeout time.Duration // ??????Leader?????????Follower??????AppendEntriesRPC
	electionTimeout  time.Duration // ??????Follower???Candidate??????????????????????????????
}

func IntMin(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
func IntMax(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	currentTerm := -1
	state := -1
	// Your code here (2A).
	rf.mu.Lock()
	currentTerm = rf.currentTerm
	state = rf.state
	rf.mu.Unlock()

	return currentTerm, state == Leader
}

func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log)-1].LogTerm
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) stateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveRaftState(rf.stateData())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Println("Decode fail")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].LogIndex
	lastIndex := rf.getLastIndex()

	if index <= baseIndex || index > lastIndex {
		return
	}
	//????????????
	var newLogEntry []LogEntry
	newLogEntry = append(newLogEntry, LogEntry{LogIndex: index, LogTerm: rf.log[index-baseIndex].LogTerm})
	for i := index + 1; i <= lastIndex; i++ {
		newLogEntry = append(newLogEntry, rf.log[i-baseIndex])
	}
	rf.log = newLogEntry
	rf.persist()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(newLogEntry[0].LogIndex)
	e.Encode(newLogEntry[0].LogTerm)
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveStateAndSnapshot(rf.stateData(), data)
}

func (rf *Raft) readSnapshot(snapshot []byte) {
	if len(snapshot) <= 0 {
		return
	}
	w := new(bytes.Buffer)
	d := labgob.NewDecoder(w)
	var LastIncludeIndex int
	var LastIncludeTerm int
	d.Decode(&LastIncludeIndex)
	d.Decode(&LastIncludeTerm)
	rf.commitIndex = LastIncludeIndex
	rf.lastApplied = LastIncludeIndex
	rf.log = TruncateLog(LastIncludeIndex, LastIncludeTerm, rf.log)

	msg := ApplyMsg{
		CommandValid: false,
	}
	go func() {
		rf.applyMsgCh <- msg
	}()

}

func TruncateLog(lastIncludeIndex int, lastIncludeTerm int, log []LogEntry) []LogEntry {
	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{LogIndex: lastIncludeIndex, LogTerm: lastIncludeTerm})
	for index := len(log) - 1; index >= 0; index-- {
		if log[index].LogIndex == lastIncludeIndex && log[index].LogTerm == lastIncludeTerm {
			newLogEntries = append(newLogEntries, log[index+1:]...)
			break
		}
	}
	return newLogEntries
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term      int  // currentTerm, for candidate to update itself
	VoteGrand bool // true means candidate received vote
}

//
// AppendEntries RPC arguments structure
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term             int        // ??????????????????
	LeaderId         int        // ????????????ID?????????????????????????????????
	PrevLogIndex     int        // ??????????????????????????????????????????
	PrevLogTerm      int        // PrevLogIndex??????????????????
	Entries          []LogEntry // ???????????????????????????????????????????????????????????????????????????????????????????????????
	LeaderCommit     int        // ???????????????????????????????????????
	LastIncludeindex int
}

//
// AppendEntries RPC reply arguments structure
// field names must start with capital letters
//
type AppendEntriesReply struct {
	Term          int // ??????????????????????????????????????????????????????????????????
	ConflictIndex int
	ConflictTerm  int
	Success       bool // ??????follower?????????????????????PreLogIndex???PreLogTerm????????????????????????
}

//
//	InstallSnapshot RPC args structure, not used now
//
type InstallSnapshotArgs struct {
	Term             int // ?????????????????????
	LeaderId         int // ????????????Id????????????follower???????????????
	LastIncludeIndex int // ????????????????????????????????????????????????
	LastIncludeTerm  int // ????????????????????????????????????????????????
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.log = TruncateLog(args.LastIncludeIndex, args.LastIncludeTerm, rf.log)

	rf.persister.SaveStateAndSnapshot(rf.stateData(), args.Data)
	rf.lastApplied = IntMax(args.LastIncludeIndex, rf.lastApplied)
	rf.commitIndex = IntMax(args.LastIncludeIndex, rf.commitIndex)
	rf.persist()

	reply.Term = args.Term
	rf.appendEntriesCh <- true

	msg := ApplyMsg{
		CommandValid: false,
	}
	rf.applyMsgCh <- msg
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	voteGrand := false
	uptoDate := false

	// ??????term<currentTerm term??????false
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGrand = voteGrand
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// ??????term>currentTerm????????????term??????????????????currentTerm?????????follower
	// ??????????????????candidate???????????????votefor???VOTENULL?????????follower
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	rf.mu.Unlock()

	// If voteFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
	// If the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// If the logs have last entries with same term, then the log with the longer index is more up-to-date.
	rf.mu.Lock()
	if args.LastLogTerm > rf.getLastTerm() {
		uptoDate = true
	}

	if args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex >= rf.getLastIndex() {
		uptoDate = true
	}

	if (rf.votedFor == VOTENULL || rf.votedFor == args.CandidateId) && uptoDate {
		voteGrand = true
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.leader = args.CandidateId
		//fmt.Println("server",rf.me,"vote to",args.CandidateId,"at term",rf.currentTerm)
		rf.voteGrantedCh <- true
	}
	reply.Term = rf.currentTerm
	reply.VoteGrand = voteGrand
	rf.persist()
	rf.mu.Unlock()
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
	//fmt.Println("server",args.CandidateId,"send request vote to",server,"at term",args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesRequest and sendAppendEntriesRequest
func (rf *Raft) AppendEntriesRequest(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	success := false
	rf.mu.Lock()
	// Leader???term??????follower???term?????????leader????????????
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = success
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// ??????leader???term?????????server???term??????????????????server???term???leader??????
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	rf.mu.Unlock()

	// ????????????part
	rf.mu.Lock()
	matchPrevLogIndex := args.PrevLogIndex - rf.log[0].LogIndex
	//fmt.Println(args.PrevLogIndex, "  ", rf.lastIncludeIndex, "   ", matchPrevLogIndex, "   ",rf.me)
	//if follower doesn't have prevLogIndex in its log,
	//it should return with conflictIndex=len(log) and conflictTerm=None
	if args.PrevLogIndex > rf.getLastIndex() {
		//fmt.Println(">>>>>")
		reply.ConflictIndex = rf.getLastIndex()
		reply.ConflictTerm = -1
	} else {
		// if a follower does have prevLogIndex in its log
		if matchPrevLogIndex == 0 || rf.log[matchPrevLogIndex].LogTerm == args.PrevLogTerm { //?????????????????????????????????????????????
			//fmt.Println("server",rf.me,"???????????????prevLogIndex is",args.PrevLogIndex,"PrevLogTerm is",args.PrevLogTerm,"???????????????",len(rf.log))
			success = true
			// ???????????????????????????????????????????????????????????????????????????server?????????????????????
			// logInsertIndex???rf.log?????????????????????????????????args.PrevLogIndex+1
			logInsertIndex := matchPrevLogIndex + 1
			// newEntriesIndex???args.Entries??????????????????rf.log????????????
			newEntriesIndex := 0
			for {
				if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if rf.log[logInsertIndex].LogTerm != args.Entries[newEntriesIndex].LogTerm {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			if newEntriesIndex < len(args.Entries) {
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				//rf.persist()
			}

			if args.LeaderCommit > rf.commitIndex {
				//???????????????????????????????????????leader?????????????????????follower?????????????????????????????????????????????????????????????????????state machine?????????????????????
				//??????follower????????????????????????commitIndex????????????????????????????????????state machine???
				rf.commitIndex = IntMin(args.LeaderCommit, rf.getLastIndex())
				rf.commitCh <- true
				//fmt.Println("server", rf.me, "update CommitIndex to", rf.commitIndex)
			}
		} else {
			// the term doesn't match, it should return conflictTerm=log[preLogIndex].Term,
			// and then search its log for the first index whose entry has term equal to conflictTerm
			if rf.log[matchPrevLogIndex].LogTerm != args.PrevLogTerm {
				reply.ConflictTerm = rf.log[matchPrevLogIndex].LogTerm
				for i := 0; i < len(rf.log); i++ {
					if rf.log[i].LogTerm == reply.ConflictTerm {
						reply.ConflictIndex = rf.log[i].LogIndex
						break
					}
				}
			}
		}
	}

	// ??????????????????????????????????????????appendEntriesCh??????true???????????????heartBeat
	rf.appendEntriesCh <- true
	rf.leader = args.LeaderId
	rf.state = Follower
	reply.Term = rf.currentTerm
	reply.Success = success
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntriesRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println("leader",rf.me,"send AppendEntriesRequest to",server,"at",args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntriesRequest", args, reply)
	return ok
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
// As for leader:
// 		???????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	term, isLeader = rf.GetState()

	rf.mu.Lock()
	if isLeader {
		index = rf.getLastIndex() + 1
		entry := LogEntry{
			LogIndex:   index,
			LogTerm:    term,
			LogCommand: command,
		}

		rf.log = append(rf.log, entry)
		rf.persist()
	}
	rf.mu.Unlock()
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

func (rf *Raft) convertToFollower(term int) {
	//fmt.Println("sever",rf.me,"convert to follower at term",term)
	rf.currentTerm = term
	rf.votedFor = VOTENULL
	rf.state = Follower
	//rf.persist()
}

func (rf *Raft) convertToCandidate() {
	// ??????state???Candidate???votedFor????????????VoteCount???1
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.leader = rf.me
	rf.voteCount = 1
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastIndex() + 1
	}
	//fmt.Println("server", rf.me, "is leader")
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
	rf.currentTerm = STARTTERM
	rf.votedFor = VOTENULL

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{LogIndex: 0, LogTerm: 0})

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = Follower

	rf.appendEntriesCh = make(chan bool, 1)
	rf.voteGrantedCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)
	rf.commitCh = make(chan bool, 1)
	rf.applyMsgCh = applyCh

	rf.heartbeatTimeout = time.Duration(HEARTBEATTIMEOUT) * time.Millisecond

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())
	rf.mu.Unlock()

	// ??????follower???candidate???leader?????????????????????
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			state := rf.state
			electionTimeout := HEARTBEATTIMEOUT*3 + rand.Intn(HEARTBEATTIMEOUT)
			rf.electionTimeout = time.Duration(electionTimeout) * time.Millisecond
			rf.mu.Unlock()

			switch state {
			case Follower:
				select {
				//If follower not received appendEntries Request or vote request, then convert to Candidate
				case <-rf.appendEntriesCh:
				case <-rf.voteGrantedCh:
				case <-time.After(rf.electionTimeout):
					rf.convertToCandidate()
				}
			case Candidate:
				go rf.leaderElection()
				select {
				case <-rf.appendEntriesCh:
				case <-rf.voteGrantedCh:
				case <-rf.leaderCh:
				case <-time.After(rf.electionTimeout):
					rf.convertToCandidate()
				}
			case Leader:
				go rf.appendEntries()
				time.Sleep(rf.heartbeatTimeout)
			}
		}
	}()

	// ??????log?????????commitCh???true????????????????????????log?????????applyMsgCh???
	go func() {
		for !rf.killed() {
			select {
			case <-rf.commitCh:
				rf.mu.Lock()
				lastApplied := rf.lastApplied
				commitIndex := rf.commitIndex
				baseIndex := rf.log[0].LogIndex
				for i := lastApplied + 1; i <= commitIndex; i++ {
					//fmt.Println("commit:  ",rf.me,"   ",i,"  ",rf.lastIncludeIndex)
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i-baseIndex].LogCommand,
						CommandIndex: rf.log[i-baseIndex].LogIndex,
					}
					//fmt.Println("server", rf.me, "apply commandIndex:", msg.CommandIndex, "command:", msg.Command, "server state is", rf.state, "current term is", rf.currentTerm)
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mu.Unlock()
			}
		}
	}()

	return rf
}

// leaderElection part
func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
	rf.mu.Unlock()

	winThreshold := len(rf.peers)/2 + 1

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			if rf.state != Candidate {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			go func(server int, voteArgs RequestVoteArgs) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &voteArgs, &reply)
				if !ok {
					//rf.mu.Lock()
					//fmt.Println("leader",args.CandidateId,"sendRequestVote to",server,"fail request term at", args.Term)
					//rf.mu.Unlock()
					return
				}

				rf.mu.Lock()
				//fmt.Println("leader",args.CandidateId,"received voteReply success from",server,"reply term is",reply.Term)
				defer rf.mu.Unlock()
				if rf.currentTerm != args.Term || rf.state != Candidate {
					return
				}

				if rf.currentTerm < reply.Term {
					rf.convertToFollower(reply.Term)
					rf.persist()
					return
				}

				if reply.VoteGrand == true {
					rf.voteCount += 1
					if rf.voteCount >= winThreshold {
						//fmt.Println("server", rf.me, "is the leader", "at term", rf.currentTerm)
						rf.convertToLeader()
						rf.leaderCh <- true
					}
				}
			}(i, args)
		}
	}
}

// appendEntries part
func (rf *Raft) appendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			baseIndex := rf.log[0].LogIndex
			rf.mu.Unlock()
			go func(server int) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// ??????lastIncludeIndex>nextIndex?????????????????????snapshot????????????
				//fmt.Println("leader:",rf.me," to server",server," baseIndex:",baseIndex,"nextIndex: ",rf.nextIndex[server])
				if baseIndex >= rf.nextIndex[server] {
					args := InstallSnapshotArgs{
						Term:             rf.currentTerm,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.log[0].LogIndex,
						LastIncludeTerm:  rf.log[0].LogTerm,
						Data:             rf.persister.ReadSnapshot(),
					}
					//fmt.Println("snapshot", rf.me, "    ", args.LastIncludeIndex)
					reply := InstallSnapshotReply{}
					ok := rf.sendSnapshot(server, &args, &reply)
					if !ok {
						return
					}
					if reply.Term > rf.currentTerm || rf.state != Leader {
						rf.convertToFollower(reply.Term)
						return
					}
					rf.matchIndex[server] = args.LastIncludeIndex
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				} else {
					prevLogIndex := rf.nextIndex[server] - 1
					prevLogTerm := rf.log[prevLogIndex-baseIndex].LogTerm
					entries := rf.log[prevLogIndex+1-baseIndex:]
					leaderCommit := rf.commitIndex
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						LeaderCommit: leaderCommit,
					}

					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntriesRequest(server, &args, &reply)

					if !ok {
						//fmt.Println("leader",args.LeaderId,"received appendReply success from",server,"reply term is",reply.Term)
						return
					}

					//fmt.Println("leader",args.LeaderId,"received appendReply success from",server,"reply term is",reply.Term)
					if reply.Term > rf.currentTerm {
						rf.convertToFollower(reply.Term)
						rf.persist()
						return
					}

					if rf.currentTerm != args.Term || rf.state != Leader {
						return
					}

					if reply.Success == false {
						//fmt.Println("conflictIndex is", reply.ConflictIndex, "conflictTerm is", reply.ConflictTerm)
						findIndex := -1
						for i := len(rf.log) - 1; i >= 0; i-- {
							if rf.log[i].LogTerm == reply.ConflictTerm {
								findIndex = rf.log[i].LogIndex
								break
							}
						}
						if findIndex == -1 {
							// if it doesn't find an entry with that term, it should set nextIndex=conflictIndex
							rf.nextIndex[server] = reply.ConflictIndex
						} else {
							// if it finds, it should set nextIndex to be findIndex+1
							rf.nextIndex[server] = findIndex + 1
						}
					} else { // success to replica log to this server or receive heartbeat successfully
						// ???????????????????????????follower???nextIndex???matchIndex
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
						//fmt.Println("server", server, "matchIndex is", rf.matchIndex[server])
						rf.nextIndex[server] = rf.matchIndex[server] + 1
					}

					// ????????????leader???commitIndex
					rf.forwardLeaderCommit()
				}
			}(i)
		}
	}
}

// ??????Leader???commitIndex
// If there exists an N such that N>commitIndex, a majority of matchIndex[i]>=N, and log[N].term==currentTerm:
// Set commitIndex=N
func (rf *Raft) forwardLeaderCommit() {
	lastIndex := rf.getLastIndex()
	baseIndex := rf.log[0].LogIndex
	N := rf.commitIndex
	for i := rf.commitIndex + 1; i <= lastIndex; i++ {
		commitNum := 1
		// rf.log[i].LogTerm==rf.currentTerm
		// ??????????????????????????????figure 8?????????????????????leader??????????????????term??????????????????????????????term????????????
		// ??????????????????????????????????????????term???????????????????????????????????????
		if rf.log[i-baseIndex].LogTerm != rf.currentTerm {
			continue
		}
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i {
				commitNum++
			}
		}
		if commitNum >= (len(rf.peers)/2 + 1) {
			N = i
		}
	}
	if N > rf.commitIndex {
		rf.commitIndex = N
		rf.commitCh <- true
	}
}
