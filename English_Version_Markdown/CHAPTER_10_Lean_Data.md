# CHAPTER 10 &emsp; Lean Data

<div align="justify">

Lean data is a simple idea: rather than collecting and curating large datasets, applications carefully select small, lean ones—just the data they need at a point in time—which are pushed from a central event store into caches, or stores they control. The resulting lightweight views are propped up by operational processes that make rederiving those views practical.


## If Messaging Remembers, Databases Don’t Have To

One interesting consequence of using event streams as a source of truth (see Chapter 9) is that any data you extract from the log doesn’t need to be stored reliably. If it is lost you can always go back for more. So if the messaging layer remembers, downstream databases don’t have to (Figure 10-1). This means you can, if you choose, regenerate a database or event-sourced view completely from the log. This might seem a bit strange. Why might you want to do that?

In the context of traditional messaging, ETL (extract, transform, load) pipelines, and the like, the messaging layer is ephemeral, and users write all messages to a database once the messages have been read. After all, they may never see them again. There are a few problems that result from this. Architectures become comparably heavyweight, with a large number of applications and services retaining copies of a large proportion of the company’s data. At a macro level, as time passes, all these copies tend to diverge from one another and data quality issues start to creep in.

Data quality issues are common and often worse than people suspect. They arise for a great many reasons. One of these is linked to our use of databases as long-lived resources that are tweaked and tuned over time, leading to inadvertently introduced errors. A database is essentially a file, and that file will be as old as the system it lives in. It will have been copied from environment to environment, and the data in the database will have been subject to many operational fixes over its lifetime. So it is unsurprising that errors and inaccuracies creep in.

In stream processing, files aren’t copied around in this way. If a stream processor creates a view, then does a release that changes the shape of that view, it typically throws the original view away, resets to offset 0, and derives a new one from the log.

Looking to other areas of our industry—DevOps and friends—we see similar patterns. There was a time when system administrators would individually tweak, tune, and mutate the computers they managed. Those computers would end up being subtly different from one another, and when things went wrong it was often hard to work out why.

Today, issues like these have been largely solved within as-a-service cultures that favor immutability through infrastructure as code. This approach comes with some clear benefits: deployments become deterministic, builds are identical, and rebuilds are easy. Suddenly ops engineers transform into happy people empowered by the predictability of the infrastructure they wield, and comforted by the certainty that their software will do exactly what it did in test.

Streaming encourages a similar approach, but for data. Event-sourced views are kept lean and can be rederived from the log in a deterministic way. The view could be a cache, a Kafka Streams state store, or a full-blown database. But for this to work, we need to deal with a problem. Loading data can be quite slow. In the next section we look at ways to keep this manageable.


<div align="center">

![Local Image](../images/chapter_10/image1.png)

*Figure 10-1. If the messaging system can store data, then the views or databases it feeds don’t have to*

</div>


## Take Only the Data You Need, Nothing More

If datasets are stored in Kafka, when you pull data into your service you can pick out just the pieces you need. This minimizes the size of the resulting views and allows them to be reshaped so that they are read-optimized. This is analogous to the way materialized views are used in relational databases to optimize for reads except, unlike in a relational database, writes and reads are decoupled. (See “Materialized Views” on page 62 in Chapter 7.)

The inventory service, discussed in Chapter 15, makes a good example. It reads inventory messages that include lots of information about the various products stored in the warehouse. When the service reads each message it throws away the vast majority of the document, stripping it back to just two fields: the product ID and the number of items in stock.

Reducing the breadth of the view keeps the dataset small and loosely coupled. By keeping the dataset small you can store more rows in the available memory or disk, and performance will typically improve as a result. Coupling is reduced too, since should the schema change, it is less likely that the service will store affected fields.

<table>
    <tr>
        <td>
            <img src="../images/note.png" width="80">
        </td>
        <td>
            If messaging remembers, derived views can be refined to contain only the data that is absolutely necessary. You can always go back for more.
        </td>
    </tr>
</table>

The approach is simple to implement in either the Kafka’s Streams DSL or KSQL, or by using the Connect API’s single message transforms feature.


## Rebuilding Event-Sourced Views

The obvious drawback of lean data is that, should you need more data, you need to go back to the log. The cleanest way to do this is to drop the view and rebuild it from scratch. If you’re using an in-memory data structure, this will happen by default, but if you’re using Kafka Streams or a database, there are other factors to consider.


### Kafka Streams

If you create event-sourced views with Kafka Streams, view regeneration is par for the course. Views are either tables, which are a direct materialization of a Kafka topic, or state stores, which are populated with the result of some declarative transformation, defined in JVM code or KSQL. Both of these are automatically rebuilt if the disk within the service is lost (or removed) or if the Streams Reset tool is invoked.1 We discussed how Kafka Streams manages statefulness in more detail in the section “The Practicalities of Being Stateful” on page 52 in Chapter 6.


### Databases and Caches

In today’s world there are many different types of databases with a wide range of performance tradeoffs. While regenerating a 50 TB Oracle database from scratch would likely be impractical, regenerating the event-sourced views used in business services is often quite workable with careful technology choice.

Because worst-case regeneration time is the limiting factor, it helps to pick a write-optimized database or cache. There are a great many options, but sensible choices include:

- An in-memory database/cache like Redis, MemSQL, or Hazelcast
- A memory-optimized database like Couchbase or one that lets you disable journaling like MongoDB
- A write/disk optimized, log-structured database like Cassandra or RocksDB


### Handling the Impracticalities of Data Movement

Rebuilding an event-sourced view may still be a relatively long-winded process (minutes or even hours!2). Because of this lead time, when releasing new software, developers typically regenerate views in advance (or in parallel), with the switch from old view to new view happening when the view is fully caught up. This is essentially the same approach taken by *stateful* stream processing applications that use Kafka Streams.

The pattern works well for simple services that need small- to medium-sized datasets, say, to drive rules engines. Working with larger datasets means slower load times. If you need to rebuild terabyte-sized datasets in a single instance of a highly indexed, disk-resident database, this load time could well be prohibitively long. But in many common cases, memory-based solutions that have fast write times, or horizontal scaling, will keep ingestion fast.


## Automation and Schema Migration

Operators typically lose trust in tools that are not regularly used. This is even more true when those scripts operate on data (database rollback scripts are notorious). So when you move from environment to environment, as part of your development workflow, it’s often best to recreate views directly from the log rather than copying database files from environment to environment as you might in a traditional database workflow (Figure 10-2).

<div align="center">

![Local Image](../images/chapter_10/image2.png)

*Figure 10-2. Data is replicated to a UAT environment where views are regenerated from source*

</div>

A good example of this is when schemas change. If you have used traditional messaging approaches before to integrate data from one system into another, you may have encountered a time when the message schema changed in a non-backward-compatible way. For example, if you were importing customer information from a messaging system into a database when the customer message schema undergoes a breaking change, you would typically craft a database script to migrate the data forward, then subscribe to the new topic of messages.

If using Kafka to hold datasets in full,3 instead of performing a schema migration, you can simply drop and regenerate the view.


## The Data Divergence Problem

In practice, all large companies start to have problems with data quality as they grow. This is actually a surprisingly deep and complex subject, with the issues that result being painstaking, laborious, and expensive to fix.

The roots of these issues come from a variety of places. Consider a typical business application that loads data from several other systems and stores that data in a database. There is a data model used on the wire (e.g., JSON), an internal domain model (e.g., an object model), a data model in the database (e.g., DDL), and finally various schemas for any outbound communication. Code needs to be written for each of these translations, and this code needs to be evolved as the various schemas change. These layers, and the understanding required for each one, introduce opportunities for misinterpretation.

Semantic issues are even trickier to address, often arising where teams, departments, or companies meet. Consider two companies going through a merger. There will be a host of equivalent datasets that were modeled differently by each side. You might think of this as a simple transformation problem, but typically there are far deeper semantic conflicts: Is a supplier a customer? Is a contractor an employee? This opens up more opportunity for misinterpretation.

So as data is moved from application to application and from service to service, these interactions behave a bit like a game of telephone (a.k.a. Chinese whispers): everything starts well, but as time passes the original message gets misinterpreted and transforms into something quite different.

Some of the resulting issues can be serious: a bank whose Risk and Finance departments disagree on the bank’s position, or a retailer—with a particularly protracted workflow—taking a week to answer customer questions. All large companies have stories like these of one form or other. So this isn’t a problem you typically face building a small web application, but it’s a problem faced by many larger, more mature architectures.

There are tried-and-tested methods for addressing these concerns. Some companies create reconciliation systems that can turn into small cottage industries of their own. In fact, there is a whole industry dedicated to combating this problem—master data management—along with a whole suite of tools for data wrangling such issues toward a shared common ground.

Streaming platforms help address these problems in a slightly different way. First, they form a kind of central nervous system that connects all applications to a single shared source of truth, reducing the telephone/Chinese whispers effect. Secondly, because data is retained immutably in the log, it’s easier to track down when an error was introduced, and it’s easier to apply fixes with the original data on hand. So while we will always make mistakes and misinterpretations, techniques like event streams as a source of truth, Command Sourcing, and lean data allow individual teams to develop the operational maturity needed to avoid mistakes in the first place, or repair the effects of them once they happen.


## Summary

So when it comes to data, we should be unequivocal about the shared facts of our system. They are the very essence of our business, after all. Lean practices encourage us to stay close to these shared facts, a process where we manufacture views that are specifically optimized to the problem space at hand. This keeps them lightweight and easier to regenerate, leveraging a similar mindset to that developed in Command Sourcing and Event Sourcing, which we discussed in Chapter 7. So while these facts may be evolved over time, applied in different ways, or recast to different contexts, they will always tie back to a single source of truth.

---

1 See *http://bit.ly/2GaCRZO* and *http://bit.ly/2IUPHJa*.

2 As a yardstick, RocksDB (which Kafka Streams uses) will bulk-load ~10M 500 KB objects per minute (roughly GbE speed). Badger will do ~4M × 1K objects a minute per SSD. Postgres will bulk-load ~1M rows per minute.

3 We assume the datasets have been migrated forward using a technique like dual-schema upgrade win‐ dow, discussed in “Handling Schema Change and Breaking Backward Compatibility” on page 124 in Chapter 13.

</div>