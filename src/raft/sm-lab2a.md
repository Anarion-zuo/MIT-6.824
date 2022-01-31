## lab2a

### TestReElection

Candidates seem to be tickling too precisely. Each time raft resets the election timer, randomize wait time.

### TestManyElections

Takes lots more time than non-sm impl. But still correct and much easier to develop.

To boost up some what efficiency, differentiate between read-only transfers and read-write transfers. MajorElected is read-only. ElectionTimeout & LargerTerm are not read-only.
