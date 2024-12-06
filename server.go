package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type       string
	Key        string
	Value      string
	ClientId   int64
	RequestNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister

	lastAppliedIndex int
	dbMap            map[string]string
	requests         map[int64]RequestResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if result, keyExists := kv.requests[args.ClientId]; keyExists {
		if args.RequestNum <= result.RequestNum {
			reply.Err = OK
			reply.Value = result.Value
			kv.mu.Unlock()
			return
		}
	}

	kv.mu.Unlock()

	op := Op{
		Type:       "Get",
		Key:        args.Key,
		Value:      "",
		ClientId:   args.ClientId,
		RequestNum: args.RequestNum,
	}

	reply.Err = kv.applyOp(op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reply.Err == OK {
		reply.Value = kv.requests[args.ClientId].Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if result, keyExists := kv.requests[args.ClientId]; keyExists {
		if args.RequestNum <= result.RequestNum {
			reply.Err = OK
			kv.mu.Unlock()
			return
		}
	}

	kv.mu.Unlock()

	op := Op{
		Type:       args.Op,
		Key:        args.Key,
		Value:      args.Value,
		ClientId:   args.ClientId,
		RequestNum: args.RequestNum,
	}

	reply.Err = kv.applyOp(op)
}

//APPLYOP NEW FUNC

func (kv *KVServer) applyOp(op Op) Err {
	_, term, _ := kv.rf.Start(op)

	for {
		if kv.killed() {
			return ErrWrongLeader
		}

		currentTerm, stillLeader := kv.rf.GetState()

		if !stillLeader || currentTerm != term {
			return ErrWrongLeader
		}

		kv.mu.Lock()
		if result, exists := kv.requests[op.ClientId]; exists && result.RequestNum == op.RequestNum {
			kv.mu.Unlock()
			break
		}

		kv.mu.Unlock()
		time.Sleep(WaitInterval)
	}

	return OK
}

//code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dbMap = make(map[string]string)
	kv.requests = make(map[int64]RequestResult)

	go kv.handleAppliedOps()

	return kv
}

//func (kv *KVServer) handleAppliedOps() {
//
//	const SleepInterval = time.Nanosecond * 1
//
//	for !kv.killed() {
//		applyChMsg := <-kv.applyCh
//
//		kv.mu.Lock()
//
//		if applyChMsg.CommandValid {
//			appliedOp := applyChMsg.Command.(Op)
//
//			result, exists := kv.requests[appliedOp.ClientId]
//			if !exists || (exists && result.RequestNum < appliedOp.RequestNum) {
//				kv.requests[appliedOp.ClientId] = RequestResult{
//					RequestNum: appliedOp.RequestNum,
//					Value:      kv.dbMap[appliedOp.Key],
//				}
//
//				if appliedOp.Type == "Put" {
//					kv.dbMap[appliedOp.Key] = appliedOp.Value
//				} else if appliedOp.Type == "Append" {
//					kv.dbMap[appliedOp.Key] += appliedOp.Value
//				}
//			}
//		}
//
//		kv.mu.Unlock()
//		time.Sleep(SleepInterval)
//	}
//}

func (kv *KVServer) handleAppliedOps() {
	for !kv.killed() {
		applyChMsg := <-kv.applyCh

		kv.mu.Lock()
		if applyChMsg.CommandValid {
			appliedOp := applyChMsg.Command.(Op)

			result, exists := kv.requests[appliedOp.ClientId]
			if !exists || result.RequestNum < appliedOp.RequestNum {
				if appliedOp.Type == "Put" {
					kv.dbMap[appliedOp.Key] = appliedOp.Value
				} else if appliedOp.Type == "Append" {
					kv.dbMap[appliedOp.Key] += appliedOp.Value
				}

				kv.requests[appliedOp.ClientId] = RequestResult{
					RequestNum: appliedOp.RequestNum,
					Value:      kv.dbMap[appliedOp.Key],
				}
			}
		}
		kv.mu.Unlock()
	}
}
