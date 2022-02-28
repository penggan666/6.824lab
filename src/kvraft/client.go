package kvraft

import (
	"../labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderId int   // 记录上次成功发送请求的LeaderID，在下次发送请求时优先向该server发送
	clerkId      int64 // Clerk ownId
	reqId        int   // Request Id
	mu           sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeaderId = 0
	ck.reqId = 0
	ck.clerkId = nrand()
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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	res := ""
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.reqId += 1
	args := GetArgs{
		Key:     key,
		ClerkId: ck.clerkId,
		ReqId:   ck.reqId,
	}
	lastLeaderId := ck.lastLeaderId

	for true {
		i := 0
		serverNum := len(ck.servers)
		for i < serverNum {
			reply := GetReply{}
			ok := ck.servers[lastLeaderId].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				ck.lastLeaderId = lastLeaderId
				res = reply.Value
				return res
			}
			lastLeaderId = (lastLeaderId + 1) % serverNum
			i += 1
		}
	}
	return res
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.reqId += 1
	args := PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkId: ck.clerkId,
		ReqId:   ck.reqId,
	}
	lastLeaderId := ck.lastLeaderId

	for true {
		i := 0
		serverNum := len(ck.servers)
		for i < serverNum {
			reply := PutAppendReply{}
			ok := ck.servers[lastLeaderId].Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				//fmt.Println(ck.clerkId,"send PutAppend request success")
				ck.lastLeaderId = lastLeaderId
				return
			}
			lastLeaderId = (lastLeaderId + 1) % serverNum
			i += 1
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	//fmt.Println("ckput",key)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
