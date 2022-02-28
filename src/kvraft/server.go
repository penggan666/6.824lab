package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string
	Key     string
	Value   string
	ClerkId int64
	ReqId   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister   *raft.Persister
	db          map[string]string
	duplicate   map[int64]int
	applyResult map[int]chan Op
}

func (kv *KVServer) Apply(entry Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch entry.Type {
	case "Put":
		kv.db[entry.Key] = entry.Value
	case "Append":
		kv.db[entry.Key] += entry.Value
	}
	kv.duplicate[entry.ClerkId] = entry.ReqId
}

// 检查请求是否重复
// 如果重复返回false，否则返回true
func (kv *KVServer) CheckDup(entry Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	version, ok := kv.duplicate[entry.ClerkId]
	if ok {
		return version >= entry.ReqId
	}
	return false
}

func (kv *KVServer) GetAgreeCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.applyResult[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.applyResult[index] = ch
	}
	return ch
}

func (kv *KVServer) AppendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)
	if !isLeader {
		return false
	}
	//fmt.Println("leader true index is",index)
	ch := kv.GetAgreeCh(index)

	select {
	case msg := <-ch: //race
		return msg == entry
	case <-time.After(1000 * time.Millisecond):
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{
		Type:    "Get",
		Key:     args.Key,
		ClerkId: args.ClerkId,
		ReqId:   args.ReqId,
	}
	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.duplicate[args.ClerkId] = args.ReqId
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{
		Type:    args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.ClerkId,
		ReqId:   args.ReqId,
	}
	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

func (kv *KVServer) EncodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.duplicate)
	return w.Bytes()
}

func (kv *KVServer) DecodeSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var lastIncludeIndex int
	var lastIncludeTerm int
	var db map[string]string
	var duplicate map[int64]int
	if d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil ||
		d.Decode(&db) !=nil ||
		d.Decode(&duplicate) !=nil{
		//fmt.Println(err1)
		//fmt.Println(err2)
	} else {
		kv.db = db
		kv.duplicate = duplicate
	}
}

func (kv *KVServer) CallRaftToSnap(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.maxraftstate == -1 || kv.maxraftstate>kv.persister.RaftStateSize(){
		return
	}
	rawSnapshot := kv.EncodeSnapshot()
	go kv.rf.Snapshot(index, rawSnapshot)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.duplicate = make(map[int64]int)
	kv.applyResult = make(map[int]chan Op)
	kv.persister = persister

	// You may need initialization code here.
	go func() {
		for !kv.killed() {
			applyMsg := <-kv.applyCh
			//fmt.Println(kv.me,"raft committed")
			//fmt.Println(applyMsg.CommandIndex)
			if applyMsg.CommandValid==true {
				op := applyMsg.Command.(Op)

				if !kv.CheckDup(op) {
					kv.Apply(op)
				}

				kv.GetAgreeCh(applyMsg.CommandIndex) <- op

				kv.CallRaftToSnap(applyMsg.CommandIndex)
			}else{
				kv.mu.Lock()
				kv.DecodeSnapshot(kv.persister.ReadSnapshot())
				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
