## 2 Approaches

Two major option: state transfer & state machine replication.

### State Transfer

The system makes a checkpoint and send its state to the backup after every operation. The whole state is preserved in the backup. This is too expansive.

### State Machine Replication

Backup operation metadata instead of the whole state. When recovering, the system starts at certain begin state, then executes all the operations stored in backup to be restored to the most recent state. This is efficient enough.

Operations are deterministic, for same operation with same data would produce the same state.

### Level of operations to replicate

Application level operations are not transparent to applications, for they are explicitly issued by the applications. 

Machine level operations are transparent, for the VM does not feel it issueing operation commands to a replicating system. The VM monitor treats all machine level instructions as operations and logs them as such.

## Overview

VM service are provided through 2 computers, one primary, the other backup. When something executes on the primary, it sends information through a logging channel to its backup. The client talks directly to the primary.

2 computers share a storage through network, provided to the guest OS as an actual disk. 

When the primary temporarily fails, the backup detects this and try to become the primary. It does testAndSet operation on a flag on the shared storage. The primary when back online does this too. Whoever succeeds on testAndSet becomes the new primary.

If one of the computer fails for too long a time, the other one as primary must find itself another backup. If both of the computer fails, we have a disaster.

## Divergence sources

- non-deterministic operation: Each time executed, the result may be different.
- input data: Given different input data, the system goes to different state.
- timer interrupts: the pace of the timer changes.

These must be dilivered in the exactly identical timing.

## Interrupts

When an interrupt arrives, backup this event to the backup server, then handle the interrupt.

## Input data

Backup the outcome of the instruction. When recovering, skip this instruction and realize the outcome manually.

## Output rule

Before the primary produce an output stuff, it must make sure whatever precedes this is safely backed-up.

This is a main issue in performance, because the primary must now wait.
