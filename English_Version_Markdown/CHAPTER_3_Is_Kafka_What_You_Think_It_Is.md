# CHAPTER 3 &emsp; Is Kafka What You Think It Is?

<div align="justify">

There is an old parable about an elephant and a group of blind men. None of the men had come across an elephant before. One blind man approaches the leg and declares, “It’s like a tree.” Another man approaches the tail and declares, “It’s like a rope.” A third approaches the trunk and declares, “It’s like a snake.” So each blind man senses the elephant from his particular point of view, and comes to a subtly different conclusion as to what an elephant is. Of course the elephant is like all these things, but it is really just an elephant!

Likewise, when people learn about Kafka they often see it from a certain viewpoint. These perspectives are usually accurate, but highlight only some subsection of the whole platform. In this chapter we look at some common points of view.


## Kafka is Like REST but Asynchronous?

Kafka provides an asynchronous protocol for connecting programs together, but it is undoubtedly a bit different from, say, TCP (transmission control protocol), HTTP, or an RPC protocol. The difference is the presence of a *broker*. A broker is a separate piece of infrastructure that broadcasts messages to any programs that are interested in them, as well as storing them for as long as is needed. So it’s perfect for streaming or fire-and-forget messaging.

Other use cases sit further from its home ground. A good example is request-response. Say you have a service for querying customer information. So you call a `getCustomer()` method, passing a `CustomerId`, and get a document describing a customer in the reply. You can build this type of request-response interaction with Kafka using two topics: one that transports the request and one that transports the response. People build systems like this, but in such cases the broker doesn’t contribute all that much. There is no requirement for broadcast. There is also no requirement for storage. So this leaves the question: would you be better off using a stateless protocol like HTTP?

So Kafka is a mechanism for programs to exchange information, but its home ground is event-based communication, where events are business facts that have value to more than one service and are worth keeping around.


## Kafka Is Like a Service Bus?

If we consider Kafka as a messaging system—with its Connect interface, which pulls data from and pushes data to a wide range of interfaces and datastores, and streaming APIs that can manipulate data in flight—it does look a little like an ESB (enterprise service bus). The difference is that ESBs focus on the integration of legacy and off-the-shelf systems, using an ephemeral and comparably low-throughput messaging layer, which encourages request-response protocols (see the previous section).

Kafka, however, is a streaming platform, and as such puts emphasis on high-throughput events and stream processing. A Kafka cluster is a distributed system at heart, providing high availability, storage, and linear scale-out. This is quite different from traditional messaging systems, which are limited to a single machine, or if they do scale outward, those scalability properties do not stretch from end to end. Tools like Kafka Streams and KSQL allow you to write simple programs that manipulate events as they move and evolve. These make the processing capabilities of a database available in the application layer, via an API, and outside the confines of the shared broker. This is quite important.

ESBs are criticized in some circles. This criticism arises from the way the technology has been built up over the last 15 years, particularly where ESBs are controlled by central teams that dictate schemas, message flows, validation, and even transformation. In practice centralized approaches like this can constrain an organization, making it hard for individual applications and services to evolve at their own pace.

ThoughtWorks called this out recently, encouraging users to steer clear of recreating the issues seen in ESBs with Kafka. At the same time, the company encouraged users to investigate event streaming as a source of truth, which we discuss in Chapter 9. Both of these represent sensible advice.

So Kafka may look a little like an ESB, but as we’ll see throughout this book, it is very different. It provides a far higher level of throughput, availability, and storage, and there are hundreds of companies routing their core facts through a single Kafka cluster. Beyond that, streaming encourages services to retain control, particularly of their data, rather than providing orchestration from a single, central team or platform. So while having one single Kafka cluster at the center of an organization is quite common, the pattern works because it is simple—nothing more than data transfer and storage, provided at scale and high availability. This is emphasized by the core mantra of event-driven services: *Centralize an immutable stream of facts. Decentralize the freedom to act, adapt, and change.*


## Kafka Is Like a Database?

Some people like to compare Kafka to a database. It certainly comes with similar features. It provides storage; production topics with hundreds of terabytes are not uncommon. It has a SQL interface that lets users define queries and execute them over the data held in the log. These can be piped into views that users can query directly. It also supports transactions. These are all things that sound quite “databasey” in nature!

So many of the elements of a traditional database are there, but if anything, Kafka is a database inside out (see “A Database Inside Out” on page 87 in Chapter 9), a tool for storing data, processing it in real time, and creating views. And while you are perfectly entitled to put a dataset in Kafka, run a KSQL query over it, and get an answer—much like you might in a traditional database—KSQL and Kafka Streams are optimized for continual computation rather than batch processing.

So while the analogy is not wholly inaccurate, it is a little off the mark. Kafka is designed to move data, operating on that data as it does so. It’s about real-time processing first, long-term storage second.


## What is Kafka Really? A Streaming Platform

As Figure 3-1 illustrates, Kafka is a streaming platform. At its core sits a cluster of Kafka brokers (discussed in detail in Chapter 4). You can interact with the cluster through a wide range of client APIs in Go, Scala, Python, REST, and more.

There are two APIs for stream processing: Kafka Streams and KSQL (which we discuss in Chapter 14). These are database engines for data in flight, allowing users to filter streams, join them together, aggregate, store state, and run arbitrary functions over the evolving dataflow. These APIs can be stateful, which means they can hold data tables much like a regular database (see “Making Services Stateful” on page 47 in Chapter 6).

The third API is Connect. This has a whole ecosystem of connectors that interface with different types of database or other endpoints, both to pull data from and push data to Kafka. Finally there is a suite of utilities—such as Replicator and Mirror Maker, which tie disparate clusters together, and the Schema Registry, which validates and manages schemas—applied to messages passed through Kafka and a number of other tools in the Confluent platform.

A streaming platform brings these tools together with the purpose of turning data at rest into data that flows through an organization. The analogy of a central nervous system is often used. The broker’s ability to scale, store data, and run without interruption makes it a unique tool for connecting many disparate applications and services across a department or organization. The Connect interface makes it easy to evolve away from legacy systems, by unlocking hidden datasets and turning them into event streams. Stream processing lets applications and services embed logic directly over these resulting streams of events.

<div align="center">

![Local Image](../images/chapter_3/image1.png)

*Figure 3-1. The core components of a streaming platform*

</div>

</div>