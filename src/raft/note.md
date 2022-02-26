## lab2B

In general, my Raft works, but may not be as the test wants it.

This is always an issue: Tester sends command to a leader who later lost the election. The command is lost because of this, but the testor does not know. This does not suggest the implementation is incorrect.

### BasicAgree test

Test assumes the following:

- leader does not change once elected
- agreement acheived in a given amount of time

Therefore, I must do the following:

- doElect more randomness
- AppendEntries sent with less time wait in between

If the first election produces a leader at term 2, this test would fail, but not because of log agreement logic. The reason is that the system would be a bit later than the first time the tester extract ApplyMsg from the channel, which results in blocking at sending to channel for quite a long time. 

### RPCBytes test

This test passes if the preceding one passes, for no particular reason.

However, the system sometimes seems to be deadlocked, given long enough time to run. The next test would reveal its reason.

If we set the number of iterations to a large enough number, the architecture dictates that the leader would run slower as its log becomes larger. Eventually, some follower starts election. A command sent to the former leader would be lost due to chance and not committed, thus the tester deems the system failed. This would also cause failure in later tests.

If we consider this a bug, the solution is to have the log architecture altered to a better form of storage.

### FailAgree test

This test reveals much that was not reveal before... (gargage talk...)

Must go back to lab2A tests, i.e. run regression tests. This is what I discovered:

- Some follower accidentally got appended entries, while none was meant. This was because followers return false when encounters larger term. Correct this then TestManyElections can run to cycle 150+.
- Remember to set correct values on log state when issueing RequestVote.
- Deadlock is a serious headache.

Then I come back working on FailAgree. 

- Must read Raft paper "commit entries from prevoius terms" to better understand the statement on tryIncrementCommitIndex.
- Must read Raft paper on "up-to-date".
- Must not wait for all AppendEntries to reply, because simulated network timeout is too long for an appropriate election timeout to be set.

On apply mechanism, I find:

- Must let a separate thread do the applying, for the purpose of convenience and nonblocking requirement.
- Must let a new thread do 'push to channel', for the purpose of nonblocking.
- Be mindful of function literals. Function literals are closures, able to read/write variables from its running context. Must pass variables as values explicitly to avoid this.
- ApplyMsg must be sent in commit order, i.e. array order. So there must only be one thread pushing the channel to ensure this.

I use https://github.com/sasha-s/go-deadlock to avoid deadlocks. All locks must be retained in the same order. So I decree that I must lock log first then lock state.

### FailNoAgree

This passes when I corrected an incorrect implementation on AppendEntries receivers.

### Test Rejoin


### ConcurrentStarts Count

This passes when `Start` is handled correctly. 

### TestBackup

- The optimization when a follower is many entries behind is not neccessary. It makes this test goes faster.
- Sometimes we get an index overflow on log index. This is because an AE is being sent after the leader meets a larger term and transferred to follower, with its out-dated log sliced off. The way to solve this is to have the initialization of RPC args and the issueing of the sending be locked together, while have the sending action take place in an async fashion. All RPCs must do this, or the state of raft when preparing RPC args and issueing RPCs would be inconsistent.
- When we do this, the issuer must not wait for all RPCs to reply, for RPCs can be handled by the sending thread and waiting would be pointless.

## lab2C

The interaction between logMachine and stateMachine is too complex, so I made them into one state machine.

Figure8Unreliable2C is likely to fail in the previous version of AppendEntries reply handling. I must change it so that nextIndex is computed based on the value of the PrevLogIndex in the corresponding args of a given reply, rather than the old value of nextIndex, because a reply may come through after a new AppendEntries is sent, by which time the nextIndex remains the same, thus the entries sent to the follower is the as many as the preceding one. I corrected this to pass this test and all other regression tests.

Lab note says I must implement fast backtracking here. I do not know if this is a MUST. I implemented it anyway.

I am using brute-force version of implementation on encoding & decoding state.

## sm problem

When an RPC handler's reply is recved by sender, the sender naturally assumes that the all state transition has already taken place in the recver. This is certainly not the case if the handler calls `issueTransfer`. Therefore, RPC handlers must call `transfer` inside themselves, to ensure that each of them is a self-contained state-transfer operation.

This resolves the cases that sometimes there are 2 leaders at the same instance.

## lab2D

After installing a snapshot, leader must populate the snapshot to all followers. This is equivalent to fast-forward a follower's log to the place of the snapshot.

To accomplish this, I tried to slow down the process of trimming logs in leader. This would not work, because there can always be a follower lagging too much behind, rendering the log compaction to no avail.

The correct way of doing this is to check a follower's state when sending AppendEntries to it, and send InstallSnapshot instead if the follower lags behind the current compacted log of the leader. It works for this test.

Be careful to pass correct parameters when calling functions whose params are adjacent and with the same type. Do not pass index to term, or vise versa.

Raft must send snapshot to service if a snapshot of the service is created last time the service runned. This would be the first message to the service even before raft actually starts follower thus candidate routines. The service can handle the message at its own pace, but most likely in its initialization process.

All Commands should be interpreted as int, though whether this limits the range of application is unknown.

Should persist lastSnapshotIndex. Find all appearances of it. Then we are done.

There is a tiny problem at when in crash tests. Cannot agree on certain things, but this happens only when raft runs for too long a time. Perhaps I will return to this in the future.

## On Apply mechanism

When doing lab3, I find a potential bug in raft that I modified accordingly.

When the server is committing too rapidly, several go routines are launched and try to send to applyCh. There is no promise which would run first, thus causes an apply-out-of-order.

The way of resolving this is simple. Let a dedicated thread handles the sending to applyCh. Let other threads notify it through condvar when there is an update is commitIndex.
