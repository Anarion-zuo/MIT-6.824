# lab3a

## basic

Append is different from Put. Put overwrites existing value to a certain key. Append appends to the value of that key.

Then we are done with TestBasic.

## TestSpeed

TestSpeed requires each kv operation be completed within 33ms in average, which means each AppendEntries should commit 3 operations. Clerks should not send to all servers and waits for replies at each command. Clerks should remember which server was leader last time and try to send to that server, until the server reports that it no longer is the leader. This does not resolve the problem. This remains to be observed.

## at most once

One difficultiy is to have each writing executed once and only once. After trying serveral different schemes on this, my current scheme is as following. One principle is that clerks must not reply on servers' replies, because replies may be lost, while calls are handled and processed correctly. When done correctly, we are through to TestOnePartition.

Each operation given to raft must be associated with information concerning when and by which server is this command submitted. A mapping between (server, opid) and commitIndex must be made. However, a clerk may be reinitialized, thus loses its opid counter with the server. Must have a way of communicating this to the server, to reset or ask for the opid counter state.

An attempted scheme is described in the next paragraph. This is not a correct implementation, because a failed reply cannot notify the clerk, who is then unaware of the execution of a certain command.

When a new command is issued by a clerk, it tells that the command was executed at log index -1, which is not executed at all. The server replies a possible executed index to the clerk. In a reissueing of the same command, the clerk tells the server this index repeatedly. The server would send the command and its executed index to raft. If raft decides to commit the command, the server checks whether the command is executed before. If the index associated to the command is -1, or is less than the maximum index of the currently executed commands, the server would execute it.

## handle uncommitted then discarded ops

Commands that are not committed, then discarded does not cause the server to reply to clerk. Set timeout mechanism on server and report the timeout back to clerk.

## persist

Must ask raft to generate snapshots.
