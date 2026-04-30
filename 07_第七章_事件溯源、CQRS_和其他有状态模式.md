# 第七章 &emsp; 事件溯源、CQRS 和其他有状态模式

<div align="justify">







<div align="center">

![Local Image](./images/chapter_7/image1.png)

*图 7-1。一个简单的订单验证工作流程*

</div>






<div align="center">

![Local Image](./images/chapter_7/image2.png)

*图 7-2。程序错误可能导致数据损坏，不仅包括服务自身的数据库，还包括服务向其他服务公开的数据*

</div>







<div align="center">

![Local Image](./images/chapter_7/image3.png)

*图 7-3。在图 7-2 所示的系统中添加 Kafka 和事件溯源方法，可确保在代码和错误执行之前保留原始事件*

</div>








<div align="center">

![Local Image](./images/chapter_7/image4.png)

*图 7-4。命令溯源提供从原始命令直接重新处理，而事件溯源仅从日志中后处理事件重新推导出服务的当前状态*

</div>






<div align="center">

![Local Image](./images/chapter_7/image5.png)

*图 7-5。订单请求已验证，并向其他服务发送了通知，但服务在数据持久化到数据库之前发生故障*

</div>







<div align="center">

![Local Image](./images/chapter_7/image6.png)

*图 7-6。当我们将事件流作为真实数据源时，通知和数据库更新都来自同一个事件，该事件被不可变地持久存储；当我们分离读写模型时，该系统就实现了 CQRS 设计模式*

</div>








<div align="center">

![Local Image](./images/chapter_7/image7.png)

*图 7-7。CQRS 支持多种读取模型，这些模型可以针对特定用例进行优化（类似于物化视图），也可以使用不同的存储技术*

</div>






<div align="center">

![Local Image](./images/chapter_7/image8.png)

*图 7-8。Kafka Streams 中的状态存储可用于在服务内部创建特定于用例的视图。*

</div>







<div align="center">

![Local Image](./images/chapter_7/image9.png)

*图 7-9。通过数据库写入事件流的示例*

</div>








<div align="center">

![Local Image](./images/chapter_7/image10.png)

*图 7-10。将写直通模式应用于 Kafka Streams 和状态存储*

</div>









<div align="center">

![Local Image](./images/chapter_7/image11.png)

*图 7-11。使用 Kafka 的 Connect API 解锁遗留数据*

</div>






<div align="center">

![Local Image](./images/chapter_7/image12.png)

*图 7-12。通过 Kafka 的 Connect API 连接到 Elasticsearch 数据库，从而实现全文搜索*

</div>







<div align="center">

![Local Image](./images/chapter_7/image13.png)

*图 7-13。MemoryImage 是一个简单的“缓存”，它将整个主题加载到服务中，以便可以在本地引用*

</div>







<div align="center">

![Local Image](./images/chapter_7/image14.png)

*图 7-14。使用 KSQL 和数据库实现的事件溯源视图；如果使用 Kafka Streams 构建，则查询和视图都位于同一层*

</div>




</div>