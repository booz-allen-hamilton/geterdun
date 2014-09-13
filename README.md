Geterdun
========

Geterdun (pronouced get-er-done) is a simple library for making sure idempotent
actions get... well, done. It provides weak ordering semantics, and will passively
try to re-process uncommitted actions in the background.

Additionally, Geterdun is able to write to any filesystem in the Hadoop API
that supports append operations, so the transaction logs that it uses to
accomplish this goal can be stored reliably.

Usage
-----

To get started, use the `GeterDun.geterDun(...)` factory method create a new
`GeterDun` instance. There is a method with a short signature and a method with
a longer signature. The method with the short signature will use sane defaults
for everything.  Then post events using `GeterDun.geterDun(EVENT event)`.
Presently events must implement `Writable`, but you can submit a pull request
to get functionality for other serialization methods :) .

Why Did You Make This
---------------------
I had a project that needed to be able to continue to ingest data while various
data stores could potentially be offline due to maintenance. I also did not
want yet another server, which itself would be subject to maintenance, monitoring,
and aggravation. I also did not need anything fancy - whether or not the
events commit in order was irrelevant, just that they would in fact eventually
be processed.

There is a dizzying array of message queues available today, but they seemed
to involve a lot more complication to map to my very simple use case than
implementing a straightforward, idempotent write-ahead log and processing
algorithm with no ordering guarantees. I was unable to find any similarly
simple write ahead logging libraries as well.

I thought about using Kafka, which is in many regards similar (although far
more complex), but decided not to mainly because I did not want to begin
operating Kafka clusters. Kafka would also involve a lot of work for me to
manage commit times and replaying, and my use case did not require strong
ordering so that seemed like a lot of hassle.

How it Works
------------
Geterdun creates a very simple write-ahead log format in the directory you
provide it. If you don't provide a directory then it will crash.

Geterdun attempts immediately to process an event that is posted to it
using the `EventProcessor` that you supply. If the action fails to process,
then one of two things happen:
 * If the action fails permenantly, by throwing an exception, then Geterdun
   will commit the action and forward it to a failure handler. You can
   specify this as well.
 * If the action fails recoverably, by returning `false` from the `EventProcessor`
   then Geterdun will attempt to reprocess the action later.

For each `GeterDun` instance, there is a background thread that is constantly
scanning the directory for log files that are not fully committed.

Whenever Geterdun fully commits a log, it deletes the file.

It is possible (although not very likely) for Geterdun to process the same event
twice. As a result the processes applied to an event should be idempotent.

Limitations
-----------
Instances of the `GeterDun` class assume they do not share directories with other
`GeterDun`s. If you want to use more than one instance you need to give them
different directories and think of how you want to coordinate them.

Currently Geterdun only supports implementations of `Writable` as events. See
previous note.

Geterdun resolves logs in memory. If you build up a volume of uncommitted
transactions that cannot fit in memory, per write ahead log, Geterdun will not be
able to recover the logs. This can be alleviated by making log rotations
more frequent.

License
-------
Apache 2.0.

See LICENSE.txt
