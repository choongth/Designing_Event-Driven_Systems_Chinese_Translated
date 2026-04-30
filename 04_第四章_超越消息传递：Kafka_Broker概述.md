# 第四章 &emsp; 超越消息传递：Kafka Broker 概述

<div align="justify">






<div align="center">

![Local Image](./images/chapter_4/image1.png)

*图 4-1。日志是仅追加的日志记录*

</div>






<div align="center">

![Local Image](./images/chapter_4/image2.png)

*图 4-2。生产者将消息分发到多台机器上的多个分区，每个分区都是一个小型队列；负载均衡的消费者（称为消费者组）共享这些分区；生产者、消费者和消费者组均受到速率限制*

</div>







<div align="center">

![Local Image](./images/chapter_4/image3.png)

*图 4-3。Kafka 中的排序由生产者使用排序键指定*

</div>







<div align="center">

![Local Image](./images/chapter_4/image4.png)

*图 4-4。如果某个服务实例发生故障，数据将被重定向，并且顺序保证得以维持*

</div>






<div align="center">

![Local Image](./images/chapter_4/image5.png)

*图 4-5。在压缩主题中，共享相同键的被取代消息将被移除。因此，在本例中，对于键 K2，消息 V2 和 V1 最终会被压缩，因为它们会被 V3 取代*

</div>













</div>