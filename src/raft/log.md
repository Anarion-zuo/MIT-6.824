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

### ConcurrentStarts Count

These pass when RPCBytes passes.

### TestBackup

This seems to be a bug in the test.
