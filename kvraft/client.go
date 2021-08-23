package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"../labrpc"
)

// Each of key/value servers ("kvservers") will have an associated Raft peer.
// Clerks send Put(), Append(), and Get() RPCs to the kvserver whose associated Raft is the leader.
// The kvserver code submits the Put/Append/Get operation to Raft, so that the Raft log holds a
// sequence of Put/Append/Get operations. All of the kvservers execute operations from the Raft log
// in order, applying the operations to their key/value databases; the intent is for the servers to
// maintain identical replicas of the key/value database.

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	clientId  int64
	requestId int64
	leader    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Each client talks to the service through a Clerk with Put/Append/Get methods.
// A Clerk manages RPC interactions with the servers
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.requestId = 0
	ck.leader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

// fetches the current value for a key. A Get for a non-existent key should return an empty string
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	for ; ; ck.leader = (ck.leader + 1) % len(ck.servers) {
		server := ck.servers[ck.leader]
		reply := GetReply{}
		ok := server.Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Command = op
	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.RequestId = ck.requestId
	ck.requestId++
	ck.mu.Unlock()

	for ; ; ck.leader = (ck.leader + 1) % len(ck.servers) {
		server := ck.servers[ck.leader]
		reply := PutAppendReply{}
		ok := server.Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
	}
}

// replaces the value for a particular key in the database
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "put")
}

// appends arg to key's value
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "append")
}
