# CHAPTER 13 &emsp; Evolving Schemas and Data over Time

<div align="justify">

Schemas are the APIs used by event-driven services, so a publisher and subscriber need to agree on exactly how a message is formatted. This creates a logical coupling between sender and receiver based on the schema they both share. In the same way that request-driven services make use of service discovery technology to discover APIs, event-driven technologies need some mechanism to discover what topics are available, and what data (i.e., schema) they provide.

There are a fair few options available for schema management: Protobuf and JSON Schema are both popular, but most projects in the Kafka space use Avro. For central schema management and verification, Confluent has an open source Schema Registry that provides a central repository for Avro schemas.


## Using Schemas to Manage the Evolution of Data in Time

Schemas provide a contract that defines what a message should look like. This is pretty intuitive. Importantly, though, most schema technologies provide a mechanism for validating whether a message in a new schema is backward-compatible with previous versions (or vice versa). This property is essential. (Don’t use Java serialization or any non-evolvable format across services that change independently.)

Say you added a “return code” field to the schema for an order; this would be a backward-compatible change. Programs running with the old schema would still be able to read messages, but they wouldn’t see the “return code” field (termed *forward compatibility*). Programs with the new schema would be able to read the whole message, with the “return code” field included (termed *backward compatibility*).

Unfortunately, you can’t move or remove fields from a schema in a compatible way, although it’s typically possible to synthesize a move with a clone. The data will be duplicated in two places until such time as a breaking change can be released.

This ability to evolve a schema with additive changes that don’t break old programs is how most shared messaging models are managed over time.

The Confluent Schema Registry can be used to police this approach. The Schema Registry provides a mapping between topics in Kafka and the schema they use (Figure 13-1). It also enforces compatibility rules before messages are added. So the Schema Registry will check every message sent to Kafka for Avro compatibility, ensuring that incompatible messages will fail on publication.

<div align="center">

![Local Image](../images/chapter_13/image1.png)

*Figure 13-1. Calling out to the Schema Registry to validate schema compatibility when reading and writing orders in the orders service*

</div>


### Handling Schema Change and Breaking Backward Compatibility

The pain of long schema migrations is one of the telltale criticisms of the relational era. But the reality is that evolving schemas are a fundamental attribute of the way data ages. The main difference between then and now is that late-bound/ schema-on-read approaches allow many incompatible data schemas to exist in the same table, topic, or the like at the same time. This pushes the problem of translating the format—from old to new—into the application layer, hence the name “schema on read.”

Schema on read turns out to be useful in a couple of ways. In many cases recent data is more valuable than older data, so programs can move forward without migrating older data they don’t really care about. This is a useful, pragmatic solution used broadly in practice, particularly with messaging. But schema on read can also be simple to implement if the parsing code for the previous schemas already exists in the codebase (which is often the case in practice).

However, whichever approach you take, unless you do a single holistic big-bang release, you will end up handling the schema-evolution problem, be it by physically migrating datasets forward or by having different application-layer routines. Kafka is no different.

As we discussed in the previous section, most of the time backward compatibility between schemas can be maintained through additive changes (i.e., new fields, but not moves or deletes). But periodically schemas will need upgrading in a non-backward-compatible way. The most common approach for this is Dual Schema Upgrade Window, where we create two topics, orders-v1 and orders-v2, for messages with the old and new schemas, respectively. Assuming orders are mastered by the orders service, this gives you a few options:

- The orders service can dual-publish in both schemas at the same time, to two topics, using Kafka’s transactions API to make the publication atomic. (This approach doesn’t solve back-population so isn’t appropriate for topics used for long-term storage.)
- The orders service can be repointed to write to orders-v2. A Kafka Streams job is added to down-convert from the orders-v2 topic to the orders-v1 for backward compatibility. (This also doesn’t solve back-population.) See Figure 13-2.
- The orders service continues to write to orders-v1. A Kafka Streams job is added that up-converts from orders-v1 topic to orders-v2 topic until all clients have upgraded, at which point the orders service is repointed to orders- v2. (This approach handles back-population.)
- The orders service can migrate its dataset internally, in its own database, then republish the whole view into the log in the orders-v2 topic. It then continues to write to both orders-v1 and orders-v2 using the appropriate formats. (This approach handles back-population.)

All four approaches achieve the same goal: to give services a window in which they can upgrade. The last two options make it easier to port historic messages from the v1 to the v2 topics, as the Kafka Streams job will do this automatically if it is started from offset 0. This makes it better suited to long-retention topics such as those used in Event Sourcing use cases.

<div align="center">

![Local Image](../images/chapter_13/image2.png)

*Figure 13-2. Dual Schema Upgrade Window: the same data coexists in two topics, with different schemas, so there is a window during which services can upgrade*

</div>

Services continue in this dual-topic mode until fully migrated to the v2 topic, at which point the v1 topic can be archived or deleted as appropriate.

As an aside, we discussed the single writer principle in Chapter 11. One of the reasons for applying this approach is that it makes schema upgrades simpler. If we had three different services writing orders, it would be much harder to schedule a non-backward-compatible upgrade without a conjoined release.


## Collaborating over Schema Change

In the previous section we discussed how to roll out a non-backward-compatible schema change. However, before such a process ensues, or even before we make a minor change to a schema, there is usually some form of team-to-team collaboration that takes place to work out whether the change is appropriate. This can take many forms. Sending an email to the affected teams, telling them what the new schema is and when it’s going live, is pretty common, as is having a central team that manages the process. Neither of these approaches works particularly well in practice, though. The email method lacks structure and accountability. The central team approach stifles progress, because you have to wait for the central team to make the change and then arrange some form of sign-off.

The best approach I’ve seen for this is to use GitHub. This works well because (a) schemas are code and should be version-controlled for all the same reasons code is, and (b) GitHub lets implementers propose a change and raise a pull request (PR), which they can code against while they build and test their system. Other interested parties can review, comment, and approve. Once consensus is reached, the PR can be merged and the new schema can be rolled out. It is this process for reliably reaching (and auditing) consensus on a change, without impeding the progress of the implementer unduly, that makes this approach the most useful option.


## Handling Unreadable Messages

Schemas aren’t always enough to ensure downstream applications will work. There is nothing to prevent a semantic error—for example, an unexpected character, invalid country code, negative quantity, or even invalid bytes (say due to corruption)—from causing a process to stall. Such errors will typically hold up processing until the issue is fixed, which can be unacceptable in some environments.

Traditional messaging systems often include a related concept called a *dead letter queue*,1 which is used to hold messages that can’t be sent, for example, because they cannot be routed to a destination queue. The concept doesn’t apply in the same way to Kafka, but it is possible for consumers to not be able to read a message, either for semantic reasons or due to the message payload being invalid (e.g., the CRC check failing, on read, as the message has become corrupted).

Some implementers choose to create a type of dead letter queue of their own in a separate topic. If a consumer cannot read a message for whatever reason, it is placed on this error queue so processing can continue. Later the error queue can be reprocessed.


## Deleting Data

When you keep datasets in the log for longer periods of time, or even indefinitely, there are times you need to delete messages, correct errors or corrupted data, or redact sensitive sections. A good example of this is recent regulations like General Data Protection Regulation (GDPR), which, among other things, gives users the right to be forgotten.

The simplest way to remove messages from Kafka is to simply let them expire. By default, Kafka will keep data for two weeks, and you can tune this to an arbitrarily large (or small) period of time. There is also an Admin API that lets you delete messages explicitly if they are older than some specified time or offset. When using Kafka for Event Sourcing or as a source of truth, you typically don’t need delete. Instead, removal of a record is performed with a null value (or delete marker as appropriate). This ensures the fully versioned history is held intact, and most Connect sinks are built with delete markers in mind.

But for regulatory requirements like GDPR, adding a delete marker isn’t enough, as all data needs to be physically removed from the system. There are a variety of approaches to this problem. Some people favor a security-based approach such as crypto shredding, but for most people, compacted topics are the tool of choice, as they allow messages to be explicitly deleted or replaced via their key.

But data isn’t removed from compacted topics in the same way as in a relational database. Instead, Kafka uses a mechanism closer to those used by Cassandra and HBase, where records are marked for removal and then later deleted when the compaction process runs. Deleting a message from a compacted topic is as simple as writing a new message to the topic with the key you want to delete and a null value. When compaction runs, the message will be deleted forever.

If the key of the topic is something other than the ```CustomerId```, then you need some process to map the two. For example, if you have a topic of Orders, then you need a mapping of customer to ```OrderId``` held somewhere. Then, to “forget” a customer, simply look up their orders and either explicitly delete them from Kafka, or alternatively redact any customer information they contain. You might roll this into a process of your own, or you might do it using Kafka Streams if you are so inclined.

There is a less common case, which is worth mentioning, where the key (which Kafka uses for ordering) is completely different from the key you want to be able to delete by. Let’s say that you need to key your orders by ProductId. This choice of key won’t let you delete orders for individual customers, so the simple method just described wouldn’t work. You can still achieve this by using a key that is a composite of the two: make the key ```[ProductId][CustomerId]```, then use a custom partitioner in the producer that extracts the ProductId and partitions only on that value. Then you can delete messages using the mechanism discussed earlier using the ```[ProductId][CustomerId]``` pair as the key.


### Triggering Downstream Deletes

Quite often you’ll be in a pipeline where Kafka is moving data from one database to another using Kafka connectors. In this case, you need to delete the record in the originating database and have that propagate through Kafka to any Connect sinks you have downstream. If you’re using CDC this will just work: the delete will be picked up by the source connector, propagated through Kafka, and deleted in the sinks. If you’re not using a CDC-enabled connector, you’ll need some custom mechanism for managing deletes.


## Segregating Public and Private Topics

When using Kafka for Event Sourcing or stream processing, in the same cluster through which different services communicate, we typically want to segregate private, internal topics from shared, business topics.

Some teams prefer to do this by convention, but you can apply a stricter segregation using the authorization interface. Essentially you assign read/write permissions, for your internal topics, only to the services that own them. This can be implemented through simple runtime validation, or alternatively fully secured via TLS or SASL.


## Summary

In this chapter we looked at a collection of somewhat disparate issues that affect event-driven systems. We considered the problem of schema change: something that is inevitable in the real-world. Often this can be managed simply by evolving the schema with a format like Avro or Protobuf that supports backward compatibility. At other times evolution will not be possible and the system will have to undergo a non-backward-compatible change. The dual-schema upgrade window is one way to handle this.

Then we briefly looked at handling unreadable messages as well as how data can be deleted. For many users deleting data won’t be an issue–it will simply age out of the log–but for those that keep data for longer periods this typically becomes important.

---

1 See *https://www.rabbitmq.com/dlx.html* and *https://ibm.co/2ui3rKO*.

</div>