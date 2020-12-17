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

import "sync"
import "labrpc"
import "time"
import "math/rand"

import "bytes"
import "encoding/gob"

// import "fmt"

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
	HEARTBEAT_INTERVAL = 50*time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Each log entry stores a state machine command along with the term number when the entry was received by the leader.
// Each log entry also has an integer index identifying its position in the log.
type LogEntry struct {
	Command	interface{}	// a state machine command
	Term	int			// the term number
	Index	int			// an index identifying its position in the log
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

	// Persistent state on all servers:
	// (Updated on stable storage before responding to RPCs)
	currentTerm	int			// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor	int			// candidateId that received vote in current term (or null if none)
	log			[]LogEntry	// log entries(first index is 1)

	// Volatile state on all servers:
	commitIndex	int	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int	// index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	// (Reinitialized after election)
	nextIndex	[]int	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex	[]int	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// state indicators
	state			int		// follower, candidate or leader
	votesCount		int		// count of votes that has received

	// channels
	heartbeatChan	chan bool	// receiving AppendEntries RPC(heartbeat) from current leader
	leaderChan		chan bool	// votes received from majority of servers: become leader
	voteGrantedChan	chan bool	// grante vote to candidate
	commitChan		chan bool	// commit log entries
}

// return index of the last log entry
func (rf *Raft) getLastEntryIndex() int {
	// fmt.Printf("Server %d: %d\n", rf.me, len(rf.log))
	return rf.log[len(rf.log)-1].Index
}

// return term of the last log entry
func (rf *Raft) getLastEntryTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	// what should be persistent
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	// what should be persistent
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	// Invoked by candidates to gather votes
	Term			int	// candidate’s term
	CandidateId		int	// candidate requesting vote
	LastLogIndex	int	// index of candidate’s last log entry
	LastLogTerm		int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term		int	// currentTerm, for candidate to update itself
	VoteGranted	bool	// true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	/*
	1. Reply false if term < currentTerm (§5.1)
	2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	*/

	// Your code here.
	rf.mu.Lock()			// lock
	defer rf.mu.Unlock()	// unlock before return
	defer rf.persist()		// persist before responding to RPCs

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	grandVote := false

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// Raft determines which of two logs is more up-to-date by comparing the index and term of the last entries in the logs.
		// If the logs have last entries with different terms, then the log with the later term is more up-to-date. 
		// If the logs end with the same term, then whichever log is longer is more up-to-date.
		receiverIndex := rf.getLastEntryIndex()
		receiverTerm := rf.getLastEntryTerm()
		if args.LastLogTerm > receiverTerm || (args.LastLogTerm == receiverTerm && args.LastLogIndex >= receiverIndex) {
			grandVote = true
		}
	}

	if grandVote {
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId

		// fmt.Printf("%d grands vote for %d in term %d\n", rf.me, args.CandidateId, rf.currentTerm)
		rf.voteGrantedChan <- true

		reply.VoteGranted = true
	}
	return
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

	rf.mu.Lock()			// lock
	defer rf.mu.Unlock()	// unlock before return


	if ok {
		// fmt.Printf("Server %d responds %d\n", server, rf.me)
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		} else if reply.VoteGranted {
			// success to get one vote
			rf.votesCount ++

			// If votes received from majority of servers: become leader
			// "==" guarantees that only send true to leaderChan once
			if rf.state == CANDIDATE && rf.votesCount == len(rf.peers)/2 + 1 {
				// fmt.Printf("VotesCount in server %d: %d\n", rf.me, rf.votesCount)
				rf.leaderChan <- true
			}
		}
	}

	return ok
}

// Candidate sends RequestVote RPC to all other servers
func (rf *Raft) sendAllRequestVote() {
	rf.mu.Lock()			// lock
	defer rf.mu.Unlock()	// unlock before return

	if rf.state != CANDIDATE {
		return
	}

	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastEntryTerm()
	args.LastLogIndex = rf.getLastEntryIndex()
	// fmt.Printf("Candidate %d send requestvote RPC to all %d other servers.\n", rf.me, len(rf.peers)-1)

	// Send RequestVote RPCs to all other servers
	for i := range rf.peers {
		if rf.state == CANDIDATE && i != rf.me {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	// Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
	Term			int			// leader’s term
	LeaderId		int			// so follower can redirect clients
	PrevLogIndex	int			// index of log entry immediately preceding new ones
	PrevLogTerm		int			// term of prevLogIndex entry
	Entries			[]LogEntry	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit	int			// leader’s commitIndex
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term		int		// currentTerm, for leader to update itself
	Success		bool	// true if follower contained entry matching prevLogIndex and prevLogTerm

	// optimize: reduce the number of rejected AppendEntries RPCs
	// when rejecting an AppendEntries request, the follower can include the term of the conflicting entry
		// and the first index it stores for that term.
	NextIndex	int
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	/*
	1. Reply false if term < currentTerm (§5.1)
	2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	4. Append any new entries not already in the log
	5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	*/
	rf.mu.Lock()			// lock
	defer rf.mu.Unlock()	// unlock before return
	defer rf.persist()		// persist before responding to RPCs

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = rf.getLastEntryIndex() + 1
		return
	}

	// Received RPC is actually from the leader
	rf.heartbeatChan <- true

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.Success = false
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// doesn’t have an entry at prevLogIndex
	lastIndex := rf.getLastEntryIndex()
	if lastIndex < args.PrevLogIndex {
		// This follower’s log is inconsistent with the leader’s
		// includes the term of the conflicting entry and the first index it stores for that term.
		reply.NextIndex = lastIndex + 1
		return
	}

	// fmt.Printf("Leader %d to Server %d: %d in %d\n", args.LeaderId, rf.me, args.PrevLogIndex, len(rf.log))
	rfTerm := rf.log[args.PrevLogIndex].Term
	if rfTerm != args.PrevLogTerm {
		// an entry at prevLogIndex whose term doesn't matche prevLogTerm
		// calculate the NextIndex: The max index whose term is not the same
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rfTerm != rf.log[i].Term {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms)
	// 		delete the existing entry and all that follow it (§5.3)
	sameLength := 0
	for i := args.PrevLogIndex + 1; i <= lastIndex && i-args.PrevLogIndex-1 < len(args.Entries); i++ {
		if rf.log[i].Index != args.Entries[i-args.PrevLogIndex-1].Index || rf.log[i].Term != args.Entries[i-args.PrevLogIndex-1].Term {
			sameLength = i - args.PrevLogIndex - 1
			break
		}
	}
	// delete the existing entry and all that follow it
	rf.log = rf.log[:args.PrevLogIndex + sameLength + 1]

	// 4. Append any new entries not already in the log
	rf.log = append(rf.log, args.Entries[sameLength:]...)

	lastIndex = rf.getLastEntryIndex()
	reply.NextIndex = lastIndex + 1
	
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		rf.commitChan <- true
	}

	reply.Success = true
	return
}

// Sends AppendEntries RPC to one server
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()			// lock
	defer rf.mu.Unlock()	// unlock before return

	/*
	• If successful: update nextIndex and matchIndex for follower (§5.3)
	• If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
	*/
	if ok {
		// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		} else if reply.Success {
			// Not only a heartbeat
			if len(args.Entries) > 0 {
				// update nextIndex and matchIndex for follower
				rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
			rf.nextIndex[server] = reply.NextIndex
		} 
	}

	return ok
}

// Leader sends AppendEntries RPC to all other servers
func (rf *Raft) sendAllAppendEntries() {
	rf.mu.Lock()			// lock
	defer rf.mu.Unlock()	// unlock before return

	if rf.state != LEADER {
		return
	}

	/*
	• If there exists an N such that N > commitIndex, a majority
	  of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	  set commitIndex = N (§5.3, §5.4).
	*/
	N := rf.commitIndex
	lastIndex := rf.getLastEntryIndex()

	// select the max N
	for i := lastIndex; i > rf.commitIndex; i-- {
		count := 1
		for j := range rf.peers {
			// matchIndex[i] ≥ N and log[N].term == currentTerm
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
				count ++
			}
		}

		// majority
		if count >= len(rf.peers)/2 + 1 {
			N = i
			break
		}
	}

	// if N > commitIndex
	if N > rf.commitIndex {
		rf.commitIndex = N
		rf.commitChan <- true
	}

	// Send AppendEntries RPCs to all other servers
	for i := range rf.peers {
		if rf.state == LEADER && i != rf.me {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.LeaderCommit = rf.commitIndex

			/*
			• If last log index ≥ nextIndex for a follower: send
			  AppendEntries RPC with log entries starting at nextIndex
			*/
			if lastIndex >= rf.nextIndex[i] {
				args.Entries = make([]LogEntry, lastIndex + 1 - rf.nextIndex[i])
				copy(args.Entries, rf.log[rf.nextIndex[i]:])
			}

			go func(i int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, args, &reply)
			}(i, args)
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()	// Lock
	defer rf.mu.Unlock()	// unlock before return

	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == LEADER)

	if isLeader {
		// the index that the command will appear at if it's ever committed
		index = rf.getLastEntryIndex() + 1
		rf.log = append(rf.log, LogEntry{Command:command, Term:term, Index:index})
		rf.persist()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{})
	rf.heartbeatChan = make(chan bool, 1)
	rf.leaderChan = make(chan bool, 1)
	rf.voteGrantedChan = make(chan bool, 1)
	rf.commitChan = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// every Raft server runs in it's state
	go func() {
		// infinite loop
		for {
			switch rf.state {
			case FOLLOWER:
				// If election timeout elapses without receiving AppendEntries
				// RPC from current leader or granting vote to candidate: convert to candidate
				select {
				// election timeout randomly from [150, 300]
				case <- time.After(time.Duration(rand.Intn(151)+150) * time.Millisecond):
					rf.mu.Lock()	// lock
					// fmt.Printf("Follower %d has become candidate.\n", rf.me)
					rf.state = CANDIDATE
					rf.mu.Unlock()	// unlock
				case <- rf.heartbeatChan:
					// fmt.Printf("Follower %d receives heartbeat.\n", rf.me)
				case <- rf.voteGrantedChan:
					// fmt.Printf("Follower %d grants vote.\n", rf.me)
				}

			case CANDIDATE:
				/* 
				• On conversion to candidate, start election:
					• Increment currentTerm
					• Vote for self
					• Reset election timer
					• Send RequestVote RPCs to all other servers
				• If votes received from majority of servers: become leader
				• If AppendEntries RPC received from new leader: convert to follower
				• If election timeout elapses: start new election
				*/
				rf.mu.Lock()	// lock
				rf.currentTerm ++
				rf.votedFor = rf.me
				rf.votesCount = 1
				rf.persist()
				rf.mu.Unlock()	// unlock

				go rf.sendAllRequestVote()
				select {
				// If votes received from majority of servers: become leader
				case <- rf.leaderChan:
					rf.mu.Lock()	// lock
					rf.state = LEADER
					rf.nextIndex = make([]int,len(rf.peers))
					rf.matchIndex = make([]int,len(rf.peers))
					for i := range rf.peers {
						// initialized to leader last log index + 1
						rf.nextIndex[i] = rf.getLastEntryIndex() + 1
						// initialized to 0, increases monotonically
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()	// unlock
					// fmt.Printf("Candidate %d become leader.\n", rf.me)
				
				// If AppendEntries RPC received from new leader: convert to follower
				case <- rf.heartbeatChan:
					rf.mu.Lock()	// lock
					// fmt.Printf("Candidate %d becomes follower after receiving heartbeat.\n", rf.me)
					rf.state = FOLLOWER
					rf.mu.Unlock()	// unlock
				
				// If election timeout elapses: start new election
				case <- time.After(time.Duration(rand.Intn(151)+150) * time.Millisecond):
				}

			case LEADER:
				/*
					• Upon election: send initial empty AppendEntries RPCs
					  (heartbeat) to each server; repeat during idle periods to
					  prevent election timeouts (§5.2)
					• If command received from client: append entry to local log,
					  respond after entry applied to state machine (§5.3)
					• If last log index ≥ nextIndex for a follower: send
					  AppendEntries RPC with log entries starting at nextIndex
						• If successful: update nextIndex and matchIndex for follower (§5.3)
						• If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
					• If there exists an N such that N > commitIndex, a majority
					  of matchIndex[i] ≥ N, and log[N].term == currentTerm:
					  set commitIndex = N (§5.3, §5.4).
				*/
				rf.sendAllAppendEntries()
				// fmt.Printf("Send heartbeat from %d\n", rf.me)
				time.Sleep(HEARTBEAT_INTERVAL)
			}
		}
	}()

	// listen for committing log entries
	go func() {
		// infinite loop
		for {
			select {
			case <- rf.commitChan:
				rf.mu.Lock()	// lock

				// fmt.Printf("base index: %d\n", rf.log[0].Index)
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					// applyCh is a channel on which the tester or service expects Raft to send ApplyMsg messages
					var messages ApplyMsg
					messages.Index = i
					messages.Command = rf.log[i].Command
					applyCh <- messages
				}
				rf.lastApplied = rf.commitIndex

				rf.mu.Unlock()	// unlock
			}
		}
	}()

	return rf
}
