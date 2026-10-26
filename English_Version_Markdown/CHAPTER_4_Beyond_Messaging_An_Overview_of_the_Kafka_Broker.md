# CHAPTER 4 &emsp; Beyond Messaging: An Overview of the Kafka Broker

<div align="justify">

A Kafka cluster is essentially a collection of files, filled with messages, spanning many different machines. Most of Kafka’s code involves tying these various individual logs together, routing messages from producers to consumers reliably, replicating for fault tolerance, and handling failure gracefully. So it is a messaging system, at least of sorts, but it’s quite different from the message brokers that preceded it. Like any technology, it comes with both pros and cons, and these shape the design of the systems we write. This chapter examines the Kafka broker (i.e., the server component) from the context of building business systems. We’ll explore a little about how it works, as well as dipping into the less conventional use cases it supports like data storage, dynamic failover, and bandwidth protection.

Originally built to distribute the datasets created by large social networks, Kafka was predominantly shaped by a need to operate at scale, in the face of failure. Accordingly, its architecture inherits more from storage systems like HDFS, HBase, or Cassandra than it does from traditional messaging systems that implement JMS (Java Message Service) or AMQP (Advanced Message Queuing Protocol).

Like many good outcomes in computer science, this scalability comes largely from simplicity. The underlying abstraction is a partitioned log—essentially a set of append-only files spread over a number of machines—which encourages sequential access patterns that naturally flow with the grain of the underlying hardware.

A Kafka cluster is a distributed system, spreading data over many machines both for fault tolerance and for linear scale-out. The system is designed to handle a range of use cases, from high-throughput streaming, where only the latest messages matter, to mission-critical use cases where messages and their relative ordering must be preserved with the same guarantees as you’d expect from a DBMS (database management system) or storage system. The price paid for this scalability is a slightly simpler contract that lacks some of the obligations of JMS or AMQP, such as message selectors.

But this change of tack turns out to be quite important. Kafka’s throughput properties make moving data from process to process faster and more practical than with previous technologies. Its ability to store datasets removes the queue-depth problems that plagued traditional messaging systems. Finally, its rich APIs, particularly Kafka Streams and KSQL, provide a unique mechanism for embedding data processing directly inside client programs. These attributes have led to its use as a message and storage backbone for service estates in a wide variety of companies that need all of these capabilities.


## The Log: An Efficient Structure for Retaining and Distributing Messages

At the heart of the Kafka messaging system sits a partitioned, replayable log. The log-structured approach is itself a simple idea: a collection of messages, appended sequentially to a file. When a service wants to read messages from Kafka, it “seeks” to the position of the last message it read, then scans sequentially, reading messages in order while periodically recording its new position in the log (see Figure 4-1).

<div align="center">

![Local Image](../images/chapter_4/image1.png)

*Figure 4-1. A log is an append-only journal*

</div>

Taking a log-structured approach has an interesting side effect. Both reads and writes are sequential operations. This makes them sympathetic to the underlying media, leveraging prefetch, the various layers of caching, and naturally batching operations together. This in turn makes them efficient. In fact, when you read messages from Kafka, the server doesn’t even import them into the JVM (Java virtual machine). Data is copied directly from the disk buffer to the network buffer (zero copy)—an opportunity afforded by the simplicity of both the contract and the underlying data structure.

So batched, sequential operations help with overall performance. They also make the system well suited to storing messages longer term. Most traditional message brokers are built with index structures—hash tables or B-trees—used to manage acknowledgments, filter message headers, and remove messages when they have been read. But the downside is that these indexes must be maintained, and this comes at a cost. They must be kept in memory to get good performance, limiting retention significantly. But the log is O(1) when either reading or writing messages to a partition, so whether the data is on disk or cached in memory matters far less.

There are a few implications to this log-structured approach. If a service has some form of outage and doesn’t read messages for a long time, the backlog won’t cause the infrastructure to slow significantly (a common problem with traditional brokers, which have a tendency to slow down as they get full). Being log- structured also makes Kafka well suited to performing the role of an event store, for those who like to apply Event Sourcing within their services. This subject is discussed in depth in Chapter 7.

<br>

> <h3 align="center">Partitions and Partitioning</h3>
>
> *Partitions* are a fundamental concept for most distributed data systems. A partition is just a bucket that data is put into, much like buckets used to group data in a hash table. In Kafka’s terminology each *log* is a *replica* of a *partition* held on a different machine. (So one partition might be replicated three times for high availability. Each replica is a separate log with the same data inside it.) What data goes into each partition is determined by a partitioner, coded into the Kafka producer. The partitioner will either spread data across the available partitions in a round-robin fashion or, if a key is provided with the message, use a hash of the key to determine the partition number. This latter point ensures that messages with the same key are always sent to the same partition and hence are strongly ordered.

<br>


## Linear Scalability

As we’ve discussed, logs provide a hardware-sympathetic data structure for messaging workloads, but Kafka is really many logs, spanning many different machines. The system ties these together, routing messages reliably, replicating for fault tolerance, and handling failure gracefully.

While running on a single machine is possible, production clusters typically start at three machines with larger clusters in the hundreds. When you read and write to a topic, you’ll typically be reading and writing to all of them, partitioning your data over all the machines you have at your disposal. Scaling is thus a pretty simple affair: add new machines and rebalance. Consumption can also be performed in parallel, with messages in a topic being spread over several consumers in a consumer group (see Figure 4-2).

<div align="center">

![Local Image](../images/chapter_4/image2.png)

*Figure 4-2. Producers spread messages over many partitions, on many machines, where each partition is a little queue; load-balanced consumers (denoted a consumer group) share the partitions between them; rate limits are applied to producers, consumers, and groups*

</div>

The main advantage of this, from an architectural perspective, is that it takes the issue of scalability off the table. With Kafka, hitting a scalability wall is virtually impossible in the context of business systems. This can be quite empowering, especially when ecosystems grow, allowing implementers to pick patterns that are a little more footloose with bandwidth and data movement.

Scalability opens other opportunities too. Single clusters can grow to company scales, without the risk of workloads overpowering the infrastructure. For example, New Relic relies on a single cluster of around 100 nodes, spanning three datacenters, and processing 30 GB/s. In other, less data-intensive domains, 5- to 10-node clusters commonly support whole-company workloads. But it should be noted that not all companies take the “one big cluster” route. Netflix, for example, advises using several smaller clusters to reduce the operational overheads of running very large installations, but their largest installation is still around the 200-node mark.

To manage shared clusters, it’s useful to carve bandwidth up, using the bandwidth segregation features that ship with Kafka. We’ll discuss these next.


## Segregating Load in Multiservice Ecosystems

Service architectures are by definition multitenant. A single cluster will be used by many different services. In fact, it’s not uncommon for all services in a company to share a single production cluster. But doing so opens up the potential for inadvertent denial-of-service attacks, causing service degradation or instability.

To help with this, Kafka includes a throughput control feature, called quotas, that allows a defined amount of bandwidth to be allocated to specific services, ensuring that they operate within strictly enforced service-level agreements, or SLAs (see Figure 4-2). Greedy services are aggressively throttled, so a single cluster can be shared by any number of services without the fear of unexpected network contention. This feature can be applied to either individual service instances or load-balanced groups.


## Maintaining Strong Ordering Guarantees

While it often isn’t the case for analytics use cases, most business systems need strong ordering guarantees. Say a customer makes several updates to their customer information. The order in which these updates are processed is going to matter, or else the latest change might be overwritten with one of the older, out- of-date values.

There are a couple of things that need to be considered to ensure strong ordering guarantees. The first is that messages that require relative ordering need to be sent to the same partition. (Kafka provides ordering guarantees only within a partition.) This is managed for you: you supply the same key for all messages that require a relative order. So a stream of customer information updates would use the CustomerId as their partitioning key. All messages for the same customer would then be routed to the same partition, and hence be strongly ordered (see Figure 4-3).

<div align="center">

![Local Image](../images/chapter_4/image3.png)

*Figure 4-3. Ordering in Kafka is specified by producers using an ordering key*

</div>

Sometimes key-based ordering isn’t enough, and global ordering is required. This often comes up when you’re migrating from legacy messaging systems where global ordering was an assumption of the original system’s design. To maintain global ordering, use a single partition topic. Throughput will be limited to that of a single machine, but this is typically sufficient for use cases of this type.

The second thing to be aware of is retries. In almost all cases we want to enable retries in the producer so that if there is some network glitch, long-running garbage collection, failure, or the like, any messages that aren’t successfully sent to the cluster will be retried. The subtlety is that messages are sent in batches, so we should be careful to send these batches one at a time, per destination machine, so there is no potential for a reordering of events when failures occur and batches are retried. This is simply something we configure.


## Ensuring Messages Are Durable

Kafka provides durability through replication. This means messages are written to a configurable number of machines so that if one or more of those machines fail, the messages will not be lost. If you configure a replication factor of three, two machines can be lost without losing data.

To make best use of replication, for sensitive datasets like those seen in service- based applications, configure three replicas for each partition and configure the producer to wait for replication to complete before proceeding. Finally, as discussed earlier, configure retries in the producer.

Highly sensitive use cases may require that data be flushed to disk synchronously, but this approach should be used sparingly. It will have a significant impact on throughput, particularly in highly concurrent environments. If you do take this approach, increase the producer batch size to increase the effectiveness of each disk flush on the machine (batches of messages are flushed together). This approach is useful for single machine deployments, too, where a single ZooKeeper node is run on the same machine and messages are flushed to disk synchronously for resilience.


## Load-Balance Services and Make Them Highly Available

Event-driven services should always be run in a highly available (HA) configuration, unless there is genuinely no requirement for HA. The main reason for this is it’s essentially a no-op. If we have one instance of a service, then start a second, load will naturally balance across the two. The same process provides high availability should one node crash (see Figure 4-4).

Say we have two instances of the orders service, reading messages from the Orders topic. Kafka would assign half of the partitions to each instance, so the load is spread over the two.

<div align="center">

![Local Image](../images/chapter_4/image4.png)

*Figure 4-4. If an instance of a service dies, data is redirected and ordering guarantees are maintained*

</div>

Should one of the services fail, Kafka will detect this failure and reroute messages from the failed service to the one that remains. If the failed service comes back online, load flips back again.

This process actually works by assigning whole partitions to different consumers. A strength of this approach is that a single partition can only ever be assigned to a single service instance (consumer). This is an invariant, implying that ordering is guaranteed, even as services fail and restart.

So services inherit both high availability and load balancing, meaning they can scale out, handle unplanned outages, or perform rolling restarts without service downtime. In fact, Kafka releases are always backward-compatible with the previous version, so you are guaranteed to be able to release a new version without taking your system offline.


## Compacted Topics

By default, topics in Kafka are retention-based: messages are retained for some configurable amount of time. Kafka also ships with a special type of topic that manages keyed datasets—that is, data that has a primary key (identifier) as you might have in a database table. These *compacted topics* retain only the most recent events, with any old events, for a certain key, being removed. They also support deletes (see “Deleting Data” on page 127 in Chapter 13).

Compacted topics work a bit like simple log-structure merge-trees (LSM trees). The topic is scanned periodically, and old messages are removed if they have been superseded (based on their key); see Figure 4-5. It’s worth noting that this is an asynchronous process, so a compacted topic may contain some superseded messages, which are waiting to be compacted away.

<div align="center">

![Local Image](../images/chapter_4/image5.png)

*Figure 4-5. In a compacted topic, superseded messages that share the same key are removed. So, in this example, for key K2, messages V2 and V1 would eventually be compacted as they are superseded by V3.*

</div>

Compacted topics let us make a couple of optimizations. First, they help us slow down a dataset’s growth (by removing superseded events), but we do so in a data-specific way rather than, say, simply removing messages older than two weeks. Second, having smaller datasets makes it easier for us to move them from machine to machine.

This is important for stateful stream processing. Say a service uses the Kafka’s Streams API to load the latest version of the product catalogue into a table (as discussed in “Windows, Joins, Tables, and State Stores” on page 135 in Chapter 14, a table is a disk resident hash table held inside the API). If the product catalogue is stored in a compacted topic in Kafka, the load can be performed quicker and more efficiently if it doesn’t have to load the whole versioned history as well (as would be the case with a regular topic).


## Long-Term Data Storage

One of the bigger differences between Kafka and other messaging systems is that it can be used as a storage layer. In fact, it’s not uncommon to see retention- based or compacted topics holding more than 100 TB of data. But Kafka isn’t a database; it’s a commit log offering no broad query functionality (and there are no plans for this to change). But its simple contract turns out to be quite useful for storing shared datasets in large systems or company architectures—for example, the use of events as a shared source of truth, as we discuss in Chapter 9.

Data can be stored in regular topics, which are great for audit or Event Sourcing, or compacted topics, which reduce the overall footprint. You can combine the two, getting the best of both worlds at the price of additional storage, by holding both and linking them together with a Kafka Streams job. This pattern is called the *latest-versioned pattern*.


## Security

Kafka provides a number of enterprise-grade security features for both authentication and authorization. Client authentication is provided through either Kerberos or Transport Layer Security (TLS) client certificates, ensuring that the Kafka cluster knows who is making each request. There is also a Unix-like permissions system, which can be used to control which users can access which data. Network communication can be encrypted, allowing messages to be securely sent across untrusted networks. Finally, administrators can require authentication for communication between Kafka and ZooKeeper.

The quotas mechanism, discussed in the section “Segregating Load in Multiservice Ecosystems” on page 21, can be linked to this notion of identity, and Kafka’s security features are extended across the different components of the Confluent platform (the Rest Proxy, Confluent Schema Registry, Replicator, etc.).


## Summary

Kafka is a little different from your average messaging technology. Being designed as a distributed, scalable infrastructure component makes it an ideal backbone through which services can exchange and buffer events. There are obviously a number of elements unique to the technology itself, but the ones that stand out are its abilities to scale, to run always on, and to retain datasets long- term.

We can use the patterns and features discussed in this chapter to build a wide variety of architectures, from fine-grained service-based systems right up to hulking corporate conglomerates. This is an approach that is safe, pragmatic, and tried and tested.

</div>