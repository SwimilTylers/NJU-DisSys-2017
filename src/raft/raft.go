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
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const InstanceSpaceSize = 5000
const ChannelSpaceSize = 256

const VoteForNone = -1

const LETimeoutLBound = 150
const LETimeoutUBound = 300

const CRDelay = 100
const CRTimeout = 800
const AERTimeout = 300

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
	Term    int
	Command interface{}
}

func (i Instance) String() string {
	return fmt.Sprintf("%d@%d", i.Command, i.Term)
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.log[:rf.lastLogIndex+1])
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	if data != nil && len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.voteFor)
		d.Decode(&rf.lastLogIndex)

		if rf.voteFor == rf.me {
			rf.voteFor = VoteForNone
		}

		bufLog := make([]*Instance, rf.lastLogIndex+1)
		d.Decode(&bufLog)
		copy(rf.log[:rf.lastLogIndex+1], bufLog)

		//DPrintln(rf.me, "Read @", rf.currentTerm, "=>", rf.voteFor, bufLog[:])
	}
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

	rChan := make(chan *RequestVoteReply, 1)
	rf.rvProcessChan <- &RequestVoteArgsReplyPair{
		Args:      &args,
		ReplyChan: rChan,
	}

	feedback := <-rChan
	reply.Term = feedback.Term
	reply.VoteGranted = feedback.VoteGranted

	// DPrintln(rf.me, "LEReply", args.CandidateId, ":", reply)
}

func (rf *Raft) decideIfGranted(args *RequestVoteArgs) bool {
	var isGranted bool

	if args.Term < rf.currentTerm {
		isGranted = false
	} else if rf.voteFor == VoteForNone || rf.voteFor == args.CandidateId {
		if rf.lastLogIndex == -1 {
			isGranted = args.LastLogIndex >= -1
		} else {
			isGranted = args.LastLogTerm > rf.log[rf.lastLogIndex].Term ||
				(args.LastLogTerm == rf.log[rf.lastLogIndex].Term && args.LastLogIndex >= rf.lastLogIndex)
			// isGranted = args.LastLogIndex >= rf.lastLogIndex && args.LastLogTerm >= rf.log[rf.lastLogIndex].Term
		}
	} else {
		isGranted = false
	}

	return isGranted
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs, rChan chan *RequestVoteReply) {
	if rf.decideIfGranted(args) {
		rf.role = Follower
		rf.voteFor = args.CandidateId
		// persist voteFor
		rf.persist()
		//DPrintln(rf.me, "vote for", rf.voteFor)

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

	//DPrintln(rf.me, "broadcast:", vote)

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
	Term              int
	Success           bool
	PossibleNextIndex int
}

type AppendEntriesArgsReplyPair struct {
	Args      *AppendEntriesArgs
	ReplyChan chan *AppendEntriesReply
}

type ReplicatingEntriesReply struct {
	Follower   int
	StartIndex int
	Len        int
	Retry      bool
	AEReply    *AppendEntriesReply
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

	rChan := make(chan *AppendEntriesReply, 1)

	rf.aeProcessChan <- &AppendEntriesArgsReplyPair{
		Args:      &args,
		ReplyChan: rChan,
	}

	feedback := <-rChan
	reply.Term = feedback.Term
	reply.Success = feedback.Success
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, rChan chan *AppendEntriesReply) bool {
	//DPrintln(rf.me, "HAE", rf.currentTerm, "##", rf.commitIndex, "/", rf.lastLogIndex, rf.log[:7], "##", args)
	var success = true
	var reset = -1
	if args.Term < rf.currentTerm {
		success = false
	} else if args.PrevLogIndex != -1 && (rf.log[args.PrevLogIndex] == nil || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// 'Reply false if log does not contain an entry at prevLogIndex whose Term matches prevLogTerm (§5.3)'
		// figure out whether it equals our if-condition
		// DPrintln(rf.me, "reject", args.PrevLogIndex+1, "because", args.PrevLogIndex != -1, rf.log[args.PrevLogIndex] == nil, rf.log[args.PrevLogIndex].Term != args.PrevLogTerm)
		success = false
		reset = rf.findResetTo(args.PrevLogTerm)
	} else if args.Entries != nil {
		startIdx := args.PrevLogIndex + 1
		//DPrintln("replica", rf.me, "receives", args.Entries, "start from", startIdx)
		for idx, entry := range args.Entries {
			relIdx := startIdx + idx

			if rf.log[relIdx] == nil {
				rf.log[relIdx] = entry
			} else if rf.log[relIdx].Term != entry.Term {
				// delete conflict entries
				for i := relIdx; i < rf.lastLogIndex; i++ {
					rf.log[i] = nil
				}
				rf.log[relIdx] = entry
				rf.lastLogIndex = relIdx
			}
		}

		// update lastLogIndex
		thisLastLogIndex := startIdx + len(args.Entries) - 1
		if thisLastLogIndex > rf.lastLogIndex {
			rf.lastLogIndex = thisLastLogIndex
			// persist lastLogIndex and log
			rf.persist()
		}
	}
	rChan <- &AppendEntriesReply{
		Term:              rf.currentTerm,
		Success:           success,
		PossibleNextIndex: reset,
	}

	return success
}

func (rf *Raft) appendOneEntry(command interface{}, crRChan chan *ClientRequestReply, reRChan chan *ReplicatingEntriesReply) {
	rf.lastLogIndex++
	rf.log[rf.lastLogIndex] = &Instance{
		Term:    rf.currentTerm,
		Command: command,
	}

	// persist lastLogIndex and log
	rf.persist()

	crRChan <- &ClientRequestReply{
		IsLeader: true,
		Term:     rf.currentTerm,
		Index:    rf.lastLogIndex + 1, // internal log starts from 0, while cfg's starts from 1
	}
	// replicating

	for id := 0; id < len(rf.peers); id++ {
		if id != rf.me {
			//DPrintln(rf.me, "replicates to", id, ":", rf.lastLogIndex, "=>", rf.log[rf.lastLogIndex])
			rf.logReplication(id, rf.nextIndex[id], reRChan)
		}
	}
}

func (rf *Raft) logReplication(toId int, nextIndex int, rChan chan *ReplicatingEntriesReply) {
	if rf.lastLogIndex < nextIndex {
		//DPrintln(rf.me, "lagging", rf.lastLogIndex, "vs.", nextIndex)
		return
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      rf.log[nextIndex : rf.lastLogIndex+1],
		LeaderCommit: rf.commitIndex,
	}

	if nextIndex > 0 {
		prevIndex := nextIndex - 1
		args.PrevLogIndex = prevIndex
		args.PrevLogTerm = rf.log[prevIndex].Term
	}

	//DPrintln("Log Replicate", args)

	go func() {
		internalRChan := make(chan *AppendEntriesReply, 1)
		go func() {
			var reply AppendEntriesReply
			rf.sendAppendEntries(toId, args, &reply)
			internalRChan <- &reply
		}()

		select {
		case aer := <-internalRChan:
			rChan <- &ReplicatingEntriesReply{
				Follower:   toId,
				StartIndex: nextIndex,
				Len:        len(args.Entries),
				Retry:      false,
				AEReply:    aer,
			}
			break
		case <-time.After(time.Duration(AERTimeout) * time.Millisecond):
			rChan <- &ReplicatingEntriesReply{
				Follower:   toId,
				StartIndex: nextIndex,
				Len:        len(args.Entries),
				Retry:      true,
				AEReply: &AppendEntriesReply{
					Term:              args.Term,
					Success:           false,
					PossibleNextIndex: -1,
				},
			}
			break
		}
	}()
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

	//DPrintln(rf.me, "bc AppendEntries:", entry)

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
		rf.bcAEVoting(rf.currentTerm, rf.lastLogIndex, rf.log[rf.lastLogIndex].Term, nil, rf.commitIndex, collectChan)
	}
	return collectChan
}

func (rf *Raft) updateCurrentTerm(term int) bool {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.voteFor = VoteForNone

		// persist currentTerm and voteFor
		rf.persist()

		//DPrintln(rf.me, "Term =", term)
		return true
	} else {
		return false
	}
}

func (rf *Raft) updateNextIndexMatchIndex(id int, val int) bool {
	var updated = false

	if rf.nextIndex[id] < val {
		updated = true
		rf.nextIndex[id] = val
	}

	if rf.matchIndex[id] < val {
		updated = true
		rf.matchIndex[id] = val
	}

	return updated
}

func (rf *Raft) updateLeaderCommitIndex() bool {
	var updated = false

	rf.matchIndex[rf.me] = rf.lastLogIndex

	matches := make([]int, len(rf.peers))

	for idx, match := range rf.matchIndex {
		matches[idx] = match
	}

	//DPrintln(rf.me, "finds matches is", matches)

	sort.Sort(sort.Reverse(sort.IntSlice(matches)))

	threshold := int(math.Ceil(float64(len(matches)+1)/2)) - 1

	if matches[threshold] > rf.commitIndex {
		rf.commitIndex = matches[threshold]
		updated = true
	}

	return updated
}

func (rf *Raft) updateCommitIndex(leaderTerm int, leaderCommit int) {
	if leaderTerm >= rf.currentTerm {
		// if a valid leader, update commitIndex
		if leaderCommit > rf.commitIndex {
			if leaderCommit > rf.lastLogIndex {
				rf.commitIndex = rf.lastLogIndex
			} else {
				rf.commitIndex = leaderCommit
			}
		}
	}
}

func (rf *Raft) findResetTo(prevTerm int) int {
	if rf.lastLogIndex == -1 {
		// no log has ever recorded
		return 0
	}

	for i := rf.lastLogIndex; i > 0; i-- {
		if rf.log[i].Term > prevTerm {
			continue
		}

		if rf.log[i].Term != rf.log[i-1].Term {
			return i
		}
	}

	return 0
}

func (rf *Raft) Exec() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		//DPrintln(rf.me, "execute", rf.lastApplied, "/", rf.commitIndex, rf.log[rf.lastApplied])
		rf.crApplyChan <- ApplyMsg{
			Index:       rf.lastApplied + 1, // modification, internal log array starts at 0, but cfg's starts at 1
			Command:     rf.log[rf.lastApplied].Command,
			UseSnapshot: false,
			Snapshot:    nil,
		}
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

		// persist currentTerm and voteFor
		rf.persist()

		timeout := time.Duration(LETimeoutLBound+rand.Intn(LETimeoutUBound-LETimeoutLBound)) * time.Millisecond

		count := 0
		threshold := int(math.Ceil(float64(len(rf.peers)+1)/2)) - 1

		collectChan := make(chan *RequestVoteReply, ChannelSpaceSize)

		if rf.lastLogIndex == -1 {
			rf.bcLEVoting(rf.currentTerm, rf.lastLogIndex, -1, collectChan)
		} else {
			rf.bcLEVoting(rf.currentTerm, rf.lastLogIndex, rf.log[rf.lastLogIndex].Term, collectChan)
		}

		var reelect = false
		for rf.role == Candidate && !reelect {
			select {
			case arPair := <-rf.rvProcessChan:
				if rf.updateCurrentTerm(arPair.Args.Term) {
					rf.role = Follower
				}
				rf.handleRequestVote(arPair.Args, arPair.ReplyChan)
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
				//DPrintln("receive AE:", args)
				if args.Term >= rf.currentTerm {
					// at least as large as currentTerm
					rf.updateCurrentTerm(args.Term)
					rf.role = Follower
					rf.voteFor = args.LeaderId
					// persist voteFor
					rf.persist()
				}

				if rf.handleAppendEntries(args, arPair.ReplyChan) {
					rf.updateCommitIndex(args.Term, args.LeaderCommit)
					rf.Exec()
				}

				break
			case cRequest := <-rf.crProcessChan:
				go func(rChan chan *ClientRequestReply) {
					time.Sleep(time.Duration(CRDelay) * time.Millisecond)
					rChan <- &ClientRequestReply{
						IsLeader: false,
					}
				}(cRequest.ReplyChan)
				break
			case <-time.After(timeout):
				// third case: timeout
				//DPrintln(rf.me, "timeout @", rf.currentTerm)
				reelect = true
				break
			}
		}
	}
}

func (rf *Raft) toBeLeader() {
	//DPrintln(rf.me, "Be a Leader @", rf.currentTerm)
	// threshold := int(math.Ceil(float64(len(rf.peers)+1)/2)) - 1

	collectChan := rf.bcHeartBeat()

	// reinitialize volatile state on leaders

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	reRChan := make(chan *ReplicatingEntriesReply, ChannelSpaceSize)

	//DPrintln(rf.me, "run leader routine @", rf.currentTerm)

	// take leader routines

	for rf.role == Leader {
		select {
		case arPair := <-rf.rvProcessChan:
			if rf.updateCurrentTerm(arPair.Args.Term) {
				rf.role = Follower
			}
			rf.handleRequestVote(arPair.Args, arPair.ReplyChan)
			break
		case arPair := <-rf.aeProcessChan:
			args := arPair.Args

			if args.Term > rf.currentTerm {
				rf.updateCurrentTerm(args.Term)
				rf.role = Follower
				rf.voteFor = args.LeaderId
				// persist voteFor
				rf.persist()
			}

			if rf.handleAppendEntries(args, arPair.ReplyChan) {
				rf.updateCommitIndex(args.Term, args.LeaderCommit)
				rf.Exec()
			}

			break
		case reply := <-collectChan:
			if rf.updateCurrentTerm(reply.Term) {
				rf.role = Follower
			}
			break
		case cRequest := <-rf.crProcessChan:
			rf.appendOneEntry(cRequest.Command, cRequest.ReplyChan, reRChan)
			break
		case reply := <-reRChan:
			if rf.updateCurrentTerm(reply.AEReply.Term) {
				rf.role = Follower
			} else if reply.AEReply.Term == rf.currentTerm {
				if reply.AEReply.Success {
					// if successfully replicated, update nextIdx and matchIdx
					upTo := reply.StartIndex + reply.Len - 1
					rf.updateNextIndexMatchIndex(reply.Follower, upTo)
					if rf.updateLeaderCommitIndex() {
						rf.Exec()
					}
				} else {
					// if not success, check if out-of-date
					id := reply.Follower

					if rf.nextIndex[id] == reply.StartIndex {
						/*
							if rf.nextIndex[id] > 0 {
								// decrement nextIndex
								rf.nextIndex[id]--
							}
						*/

						//DPrintln(reply, "$=>", reply.AEReply)
						if !reply.Retry {
							rf.nextIndex[id] = reply.AEReply.PossibleNextIndex
							rf.matchIndex[id] = 0

							//DPrintln("Replication failed: from", id, "startIndex is", reply.StartIndex, "reset to", rf.nextIndex[id])
						} else {
							//DPrintln("Replication restart: from", id, "startIndex is", reply.StartIndex)
						}
						// retry
						rf.logReplication(id, rf.nextIndex[id], reRChan)
					}
				}
			}
			break
		case <-time.After(time.Duration(HBInterval) * time.Millisecond):
			collectChan = rf.bcHeartBeat()
			break
		}
	}
}

func (rf *Raft) toBeFollower() {
	//DPrintln(rf.me, "Be a follower")

	for rf.role == Follower {
		select {
		case arPair := <-rf.rvProcessChan:
			rf.updateCurrentTerm(arPair.Args.Term)
			rf.handleRequestVote(arPair.Args, arPair.ReplyChan)
			break
		case arPair := <-rf.aeProcessChan:
			rf.updateCurrentTerm(arPair.Args.Term)
			if rf.handleAppendEntries(arPair.Args, arPair.ReplyChan) {
				rf.updateCommitIndex(arPair.Args.Term, arPair.Args.LeaderCommit)
				rf.Exec()
			}
			break
		case cRequest := <-rf.crProcessChan:
			go func(rChan chan *ClientRequestReply) {
				time.Sleep(time.Duration(CRDelay) * time.Millisecond)
				rChan <- &ClientRequestReply{
					IsLeader: false,
				}
			}(cRequest.ReplyChan)
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

	//DPrintln(rf.me, "receive request", command)

	rChan := make(chan *ClientRequestReply, 1)

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
		//DPrintln("Request no response:", command, "@", rf.me)
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
		rvProcessChan: make(chan *RequestVoteArgsReplyPair, ChannelSpaceSize),
		aeProcessChan: make(chan *AppendEntriesArgsReplyPair, ChannelSpaceSize),
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
