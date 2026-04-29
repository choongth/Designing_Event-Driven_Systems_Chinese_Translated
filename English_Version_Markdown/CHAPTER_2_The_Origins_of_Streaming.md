# CHAPTER 2 &emsp; The Origins of Streaming

<div align="justify">

This book is about building business systems with stream processing tools, so it is useful to have an appreciation for where stream processing came from. The maturation of this toolset, in the world of real-time analytics, has heavily influenced the way we build event-driven systems today.

Figure 2-1 shows a stream processing system used to ingest data from several hundred thousand mobile devices. Each device sends small JSON messages to denote applications on each mobile phone that are being opened, being closed, or crashing. This can be used to look for instability—that is, where the ratio of crashes to usage is comparatively high.

<div align="center">

![Local Image](../images/chapter_2/image1.png)

*Figure 2-1. A typical streaming application that ingests data from mobile devices into Kafka, processes it in a streaming layer, and then pushes the result to a serving layer where it can be queried*

</div>

The mobile devices land their data into Kafka, which buffers it until it can be extracted by the various applications that need to put it to further use. For this type of workload the cluster would be relatively large; as a ballpark figure Kafka ingests data at network speed, but the overhead of replication typically divides that by three (so a three-node 10 GbE cluster will ingest around 1 GB/s in practice).

To the right of Kafka in Figure 2-1 sits the stream processing layer. This is a clustered application, where queries are either defined up front via the Java DSL or sent dynamically via KSQL, Kafka’s SQL-like stream processing language. Unlike in a traditional database, these queries compute continuously, so every time an input arrives in the stream processing layer, the query is recomputed, and a result is emitted if the value of the query has changed.

Once a new message has passed through all streaming computations, the result lands in a serving layer from which it can be queried. Cassandra is shown in Figure 2-1, but pushing to HDFS (Hadoop Distributed File System), pushing to another datastore, or querying directly from Kafka Streams using its interactive queries feature are all common approaches as well.

To understand streaming better, it helps to look at a typical query. Figure 2-2 shows one that computes the total number of app crashes per day. Every time a new message comes in, signifying that an application crashed, the count of total crashes for that application will be incremented. Note that this computation requires state: the count for the day so far (i.e., within the window duration) must be stored so that, should the stream processor crash/restart, the count will continue where it was before. Kafka Streams and KSQL manage this state internally, and that state is backed up to Kafka via a *changelog topic*. This is discussed in more detail in “Windows, Joins, Tables, and State Stores” on page 135 in Chapter 14.

<div align="center">

![Local Image](../images/chapter_2/image2.png)

*Figure 2-2. A simple KSQL query that evaluates crashes per day*

</div>

Multiple queries of this type can be chained together in a pipeline. In Figure 2-3, we break the preceding problem into three steps chained over two stages. Queries (a) and (b) continuously compute apps opened per day and apps crashed per day, respectively. The two resulting output streams are combined together in the final stage (c), which computes application stability by calculating the ratio between crashes and usage and comparing it to a fixed bound.

<div align="center">

![Local Image](../images/chapter_2/image3.png)

*Figure 2-3. Two initial stream processing queries are pushed into a third to create a pipeline*

</div>

There are a few other things to note about this streaming approach:

*The streaming layer is fault-tolerant*  
It runs as a cluster on all available nodes. If one node exits, another will pick up where it left off. Likewise, you can scale out the cluster by adding new processing nodes. Work, and any required state, will automatically be rerouted to make use of these new resources.

*Each stream processor node can hold state of its own*  
This is required for buffering as well as holding whole tables, for example, to do enrichments (streams and tables are discussed in more detail in “Windows, Joins, Tables, and State Stores” on page 135 in Chapter 14). This idea of local storage is important, as it lets the stream processor perform fast, message-at-a-time queries without crossing the network—a necessary feature for the high-velocity workloads seen in internet-scale use cases. But this ability to internalize state in local stores turns out to be useful for a number of business-related use cases too, as we discuss later in this book.

*Each stream processor can write and store local state*  
Making message-at-a-time network calls isn’t a particularly good idea when you’re handling a high-throughput event stream. For this reason stream processors write data locally (so writes and reads are fast) and back those writes up to Kafka. So, for example, the aforementioned count requires a running total to be tracked so that, should a crash and restart occur, the computation resumes from its previous position and the count remains accurate. This ability to store data locally is very similar conceptually to the way you might interact with a database in a traditional application. But unlike in a traditional two-tier application, where interacting with the database means making a network call, in stream processing all the state is local (you might think of it as a kind of cache), so it is fast to access—no network calls needed. Because it is also flushed back to Kafka, it inherits Kafka’s durability guarantees. We discuss this in more detail in “Scaling Concurrent Operations in Streaming Systems” on page 142 in Chapter 15.

</div>