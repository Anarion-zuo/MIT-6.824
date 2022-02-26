# lab3a

## basic

Append is different from Put. Put overwrites existing value to a certain key. Append appends to the value of that key.

Then we are done with TestBasic.

## TestSpeed

TestSpeed requires each kv operation be completed within 33ms in average, which means each AppendEntries should commit 3 operations. Clerks should not send to all servers and waits for replies at each command. Clerks should remember which server was leader last time and try to send to that server, until the server reports that it no longer is the leader. This does not resolve the problem. This remains to be observed.

The way to resolve this is to have Raft.Start send AppendEntries RPC immediately if a command can be appended to its log. Whether it should clear AppendEntries timer is of no big issue here.

## at most once

One difficultiy is to have each writing executed once and only once. After trying serveral different schemes on this, my current scheme is as following. One principle is that clerks must not reply on servers' replies, because replies may be lost, while calls are handled and processed correctly. When done correctly, we are through to Unreliable tests.

Each operation given to raft must be associated with information concerning when and by which server is this command submitted. A mapping between (clerkid, opid) and commitIndex must be made. However, a clerk may be reinitialized, thus loses its opid counter with the server. Must have a way of communicating this to the server, to reset or ask for the opid counter state. The way to do this is to have a clerkId generated at each initialization of a clerk, making opid mapped to a different clerkid. If apply routine finds that a write command has been executed before, by looking up the operation record corresponding to the key, and update the record accordingly.

An attempted scheme is described in the next paragraph. This is not a correct implementation, because a failed reply cannot notify the clerk, who is then unaware of the execution of a certain command.

When a new command is issued by a clerk, it tells that the command was executed at log index -1, which is not executed at all. The server replies a possible executed index to the clerk. In a reissueing of the same command, the clerk tells the server this index repeatedly. The server would send the command and its executed index to raft. If raft decides to commit the command, the server checks whether the command is executed before. If the index associated to the command is -1, or is less than the maximum index of the currently executed commands, the server would execute it.

## TestOnePartition

Must not set timeout to too large a value or this would not pass.

## read issue

Get rpc must be handled in much the same way as PutAppend. If the rpc handler reads from kv and returns the read state immediately, it might observe a state before previous PutAppend rpcs run through. It must wait for commitIndex to grow, and the best way of doing this is to have itself inserted into raft.

Get can be executed twice and without effecting the system. The problem is the timing.

## handle uncommitted then discarded ops

Commands that are not committed, then discarded does not cause the server to reply to clerk. Set timeout mechanism on server and report the timeout back to clerk, or rpc handlers are blocked indefinitely.

## crash & persist

Need no snapshot yet. Raft can play through logs from its persistent. The problem is that under Concurrent tests, some Get gets intermediate states. That is, raft has not run through log while server returns the log not through.

This is probably because the reading of state in RPC handler is not the same time as the execution of the command. Therefore, a possible solution to this is to transmit the required state in the execution of certain command.

Although what is described above is an important issue, it is not the cause of the problem. I formerly adopted a mechanism to generate an id for each rpc request and map it to certain condvar or channel. This is flawed because server generates ids from 0 or 1 or any given value at each restart, so applying routine cannot differentiate between old and new requests. The correct way of handling this is to map commit index to condvars or channels, as each valid request is unique in its commit index across restarts. It must ensure that condvars are initialized before applying routine can broadcast. This is done by locking the calling of Raft.Start and cond.Broadcast with the same lock.

## snapshot

When I first begin working on kvraft's snapshot, I do not fully comprehend the architecture of raft's snapshot. The snapshot parameter passed into Snapshot and CondSnapshot is for the storing of the snapshot of the service, not for validation of log entries. The byte array in ApplyMsg is for the sending of a snapshot to the service.
