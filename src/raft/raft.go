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
	"math"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const InstanceSpaceSize = 5000
const ChannelSpaceSize = 32

const VoteForNone = -1

const LETimeoutLBound = 150
const LETimeoutUBound = 300

const CRTimeout = 500

const HBInterval = 50
const HBTimeout = HBInterval * 3

const (
	Follower uint8 = iota
	Leader
	Candidate
)

//
// instance
//
type Instance struct {
	term        int
	something   interface{}
	isCommitted bool
}

type ClientRequestReply struct {
	IsLeader bool
	Term     int
	Index    int
}

type ClientRequestArgsReplyPair struct {
	Command   interface{}
	ReplyChan chan *ClientRequestReply
}

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent states
	currentTerm  int
	voteFor      int
	log          []*Instance
	lastLogIndex int

	// volatile states, for followers
	commitIndex int
	lastApplied int
	role        uint8

	rvProcessChan chan *RequestVoteArgsReplyPair
	aeProcessChan chan *AppendEntriesArgsReplyPair

	// volatile states, for leaders
	nextIndex  []int
	matchIndex []int

	// client com
	crProcessChan chan *ClientRequestArgsReplyPair
	crApplyChan   chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.role == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type RequestVoteArgsReplyPair struct {
	Args      *RequestVoteArgs
	ReplyChan chan *RequestVoteReply
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	/*
		rf.rvArgsChan <- &args
		feedback := <-rf.rvArgsReplyChan
	*/

	rChan := make(chan *RequestVoteReply)
	rf.rvProcessChan <- &RequestVoteArgsReplyPair{
		Args:      &args,
		ReplyChan: rChan,
	}

	feedback := <-rChan
	reply.Term = feedback.Term
	reply.VoteGranted = feedback.VoteGranted

	DPrintln(rf.me, "LEReply", args.CandidateId, ":", reply)
}

func (rf *Raft) decideIfGranted(args *RequestVoteArgs) bool {
	var isGranted bool

	if args.Term < rf.currentTerm {
		isGranted = false
	} else if rf.voteFor == VoteForNone || rf.voteFor == args.CandidateId {
		if rf.lastLogIndex == -1 {
			isGranted = args.LastLogIndex >= -1
		} else {
			isGranted = args.LastLogTerm > rf.log[rf.lastLogIndex].term ||
				(args.LastLogTerm == rf.log[rf.lastLogIndex].term && args.LastLogIndex >= rf.lastLogIndex)
			// isGranted = args.LastLogIndex >= rf.lastLogIndex && args.LastLogTerm >= rf.log[rf.lastLogIndex].term
		}
	} else {
		isGranted = false
	}

	return isGranted
}

func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, rChan chan *RequestVoteReply) {
	if rf.decideIfGranted(args) {
		rf.role = Follower
		rf.voteFor = args.CandidateId
		DPrintln(rf.me, "vote for", rf.voteFor)

		rChan <- &RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: true,
		}
	} else {
		rChan <- &RequestVoteReply{
			Term:        rf.currentTerm,
			VoteGranted: false,
		}
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) bcLEVoting(term int, lastLogIndex int, lastLogTerm int, collectChan chan *RequestVoteReply) {
	vote := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	DPrintln(rf.me, "broadcast:", vote)

	for toId := range rf.peers {
		go func(to int) {
			if to != rf.me {
				var reply RequestVoteReply
				rf.sendRequestVote(to, vote, &reply)
				collectChan <- &reply
			}
		}(toId)
	}
}

//
// example AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Instance
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here.
	Term    int
	Success bool
}

type AppendEntriesArgsReplyPair struct {
	Args      *AppendEntriesArgs
	ReplyChan chan *AppendEntriesReply
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	/*
		rf.aeArgsChan <- &args
		feedback := <-rf.aeArgsReplyChan
	*/

	rChan := make(chan *AppendEntriesReply)

	rf.aeProcessChan <- &AppendEntriesArgsReplyPair{
		Args:      &args,
		ReplyChan: rChan,
	}

	feedback := <-rChan
	reply.Term = feedback.Term
	reply.Success = feedback.Success
}

func (rf *Raft) HandleAppendEntries(args *AppendEntriesArgs, rChan chan *AppendEntriesReply) {
	var success = true
	if args.Term < rf.currentTerm {
		success = false
	} else if args.PrevLogIndex != -1 && (rf.log[args.PrevLogIndex] == nil || rf.log[args.PrevLogIndex].term != args.PrevLogTerm) {
		// todo: check the meaning of the following description:
		// 'Reply false if log does not contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)'
		// figure out whether it equals our if-condition
		success = false
	} else if args.Entries != nil {
		startIdx := args.PrevLogIndex + 1

		for idx, entry := range args.Entries {
			relIdx := startIdx + idx

			if rf.log[relIdx] == nil {
				rf.log[relIdx] = entry
			} else if rf.log[relIdx].term != entry.term {
				// delete conflict entries
				for i := relIdx; i < rf.lastLogIndex; i++ {
					rf.log[i] = nil
				}
				rf.log[relIdx] = entry
			}
		}

		// update lastLogIndex
		rf.lastLogIndex = startIdx + len(args.Entries) - 1

		// update commitIndex
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > rf.lastLogIndex {
				rf.commitIndex = rf.lastLogIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}
	}
	rChan <- &AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: success,
	}
}

func (rf *Raft) AppendOneEntry(command interface{}, crRChan chan *ClientRequestReply) {
	rf.lastLogIndex++
	rf.log[rf.lastLogIndex] = &Instance{
		term:        rf.currentTerm,
		something:   command,
		isCommitted: false,
	}

	crRChan <- &ClientRequestReply{
		IsLeader: true,
		Term:     rf.currentTerm,
		Index:    rf.lastLogIndex,
	}

	// todo: propose ?
}

//
// example code to send a AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) bcAEVoting(term int, prevLogIndex int, prevLogTerm int, entries []*Instance, leaderCommit int, collectChan chan *AppendEntriesReply) {
	entry := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	DPrintln(rf.me, "bc AppendEntries:", entry)

	for toId := range rf.peers {
		go func(to int) {
			if to != rf.me {
				var reply AppendEntriesReply
				rf.sendAppendEntries(to, entry, &reply)
				collectChan <- &reply
			}
		}(toId)
	}
}

func (rf *Raft) bcHeartBeat() chan *AppendEntriesReply {
	collectChan := make(chan *AppendEntriesReply, ChannelSpaceSize)
	if rf.lastLogIndex == -1 {
		rf.bcAEVoting(rf.currentTerm, rf.lastLogIndex, -1, nil, rf.commitIndex, collectChan)
	} else {
		rf.bcAEVoting(rf.currentTerm, rf.lastLogIndex, rf.log[rf.lastLogIndex].term, nil, rf.commitIndex, collectChan)
	}
	return collectChan
}

func (rf *Raft) updateCurrentTerm(term int) bool {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = VoteForNone
		DPrintln(rf.me, "term =", term)
		return true
	} else {
		return false
	}
}

//
// roles
//

func (rf *Raft) toBeCandidate() {
	for rf.role == Candidate {
		// start leader election
		rf.currentTerm++
		rf.voteFor = rf.me
		timeout := time.Duration(LETimeoutLBound+rand.Intn(LETimeoutUBound-LETimeoutLBound)) * time.Millisecond

		count := 0
		threshold := int(math.Ceil(float64(len(rf.peers)+1)/2)) - 1

		collectChan := make(chan *RequestVoteReply, ChannelSpaceSize)

		if rf.lastLogIndex == -1 {
			rf.bcLEVoting(rf.currentTerm, rf.lastLogIndex, -1, collectChan)
		} else {
			rf.bcLEVoting(rf.currentTerm, rf.lastLogIndex, rf.log[rf.lastLogIndex].term, collectChan)
		}

		var reelect = false
		for rf.role == Candidate && !reelect {
			select {
			case arPair := <-rf.rvProcessChan:
				if rf.updateCurrentTerm(arPair.Args.Term) {
					rf.role = Follower
				}
				rf.HandleRequestVote(arPair.Args, arPair.ReplyChan)
				break
			case reply := <-collectChan:
				if rf.updateCurrentTerm(reply.Term) {
					rf.role = Follower
				} else if reply.VoteGranted {
					count++
					if count == threshold {
						rf.role = Leader
						rf.bcHeartBeat()
					}
				}
				break
			case arPair := <-rf.aeProcessChan:
				args := arPair.Args

				// second case: receive AE from new leader
				DPrintln("receive AE:", args)
				if args.Term == rf.currentTerm || rf.updateCurrentTerm(args.Term) {
					// at least as large as currentTerm
					rf.role = Follower
					rf.voteFor = args.LeaderId
				}

				rf.HandleAppendEntries(args, arPair.ReplyChan)

				break
			case cRequest := <-rf.crProcessChan:
				cRequest.ReplyChan <- &ClientRequestReply{
					IsLeader: false,
				}
				break
			case <-time.After(timeout):
				// third case: timeout
				DPrintln("timeout")
				reelect = true
				break
			}
		}
	}
}

func (rf *Raft) toBeLeader() {
	DPrintln(rf.me, "Be a Leader")
	// threshold := int(math.Ceil(float64(len(rf.peers)+1)/2)) - 1

	collectChan := rf.bcHeartBeat()

	// reinitialize volatile state on leaders

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	// take leader routines

	for rf.role == Leader {
		select {
		case arPair := <-rf.rvProcessChan:
			if rf.updateCurrentTerm(arPair.Args.Term) {
				rf.role = Follower
			}
			rf.HandleRequestVote(arPair.Args, arPair.ReplyChan)
			break
		case arPair := <-rf.aeProcessChan:
			args := arPair.Args

			if rf.updateCurrentTerm(args.Term) {
				rf.role = Follower
				rf.voteFor = args.LeaderId
			}
			rf.HandleAppendEntries(args, arPair.ReplyChan)
			break
		case reply := <-collectChan:
			if rf.updateCurrentTerm(reply.Term) {
				rf.role = Follower
			}
			break
		case cRequest := <-rf.crProcessChan:
			rf.AppendOneEntry(cRequest.Command, cRequest.ReplyChan)
			break
		case <-time.After(time.Duration(HBInterval) * time.Millisecond):
			collectChan = rf.bcHeartBeat()
			break
		}
	}
}

func (rf *Raft) toBeFollower() {
	DPrintln(rf.me, "Be a follower")

	for rf.role == Follower {
		select {
		case arPair := <-rf.rvProcessChan:
			rf.updateCurrentTerm(arPair.Args.Term)
			rf.HandleRequestVote(arPair.Args, arPair.ReplyChan)
			break
		case arPair := <-rf.aeProcessChan:
			rf.updateCurrentTerm(arPair.Args.Term)
			rf.HandleAppendEntries(arPair.Args, arPair.ReplyChan)
			break
		case cRequest := <-rf.crProcessChan:
			cRequest.ReplyChan <- &ClientRequestReply{
				IsLeader: false,
			}
			break
		case <-time.After(time.Duration(HBTimeout) * time.Millisecond):
			rf.role = Candidate
			break
		}
	}
}

func (rf *Raft) run() {
	for {
		switch rf.role {
		case Follower:
			rf.toBeFollower()
			break
		case Leader:
			rf.toBeLeader()
			break
		case Candidate:
			rf.toBeCandidate()
			break
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	rChan := make(chan *ClientRequestReply)

	rf.crProcessChan <- &ClientRequestArgsReplyPair{
		Command:   command,
		ReplyChan: rChan,
	}

	select {
	case reply := <-rChan:
		if reply.IsLeader {
			index = reply.Index
			term = reply.Term
			isLeader = true
		} else {
			isLeader = false
		}
		break
	case <-time.After(time.Duration(CRTimeout) * time.Millisecond):
		DPrintln("Request no response:", command, "@", rf.me)
		break
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	// Your initialization code here.
	rf := &Raft{
		mu:            sync.Mutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		currentTerm:   0,
		voteFor:       VoteForNone,
		log:           make([]*Instance, InstanceSpaceSize),
		lastLogIndex:  -1,
		commitIndex:   -1,
		lastApplied:   -1,
		role:          Follower,
		rvProcessChan: make(chan *RequestVoteArgsReplyPair),
		aeProcessChan: make(chan *AppendEntriesArgsReplyPair),
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		crProcessChan: make(chan *ClientRequestArgsReplyPair, ChannelSpaceSize),
		crApplyChan:   applyCh,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
