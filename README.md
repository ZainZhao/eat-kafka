# eat-kakfa
专注于 Kafka 面试题

> 整个 eat 系列 都以问题为基础，尽量简洁，重点在直接告诉你  “**相关问题应该回答哪些点**”
>
> 当然，我也是整理的，资料来源于网络，难免出错，有问题可以 issue





## **基础篇**

什么是消息引擎，消息引擎的作用？

```markdown
- 传递消息
    点对点模型
    发布订阅模型
- 解耦削峰
```

Kafka 术语解释

```markdown
- 三层消息架构
	主题、分区、消息

- Broker
	Kafka服务器端指Broker服务进程, 一个Kafka集群由多个Broker组成
	Broker 负责接收和处理客户端发送过来的请求，以及对消息进行持久化
	
- Replication
	备份机制、副本（Replica）
		领导者副本（Leader Replica）：对外提供服务，与客户端交互。生产者向领导者副本写消息，消费者从领导者副本读消息。
		追随者副本（Follower Replica）：不对外提供服务，被动追随领导者副本。这里与mysql不同，mysql的从库是可以处理读操作的。现在将Master-Slave改为Leader-Follower（防止歧视，哈哈）。

- Partitioning
	分区：类似MongoDB和Elasticsearch中的Sharding、HBase中的Region
	将每个Topic划分成多个分区（Partition），每个分区是一组有序的消息日志。
		生产者生产的每条消息只会被发送到一个分区中
		分区编号从0开始
	每个分区可以配置多个副本，但只能有1个领导者副本

- 持久化
	使用消息日志Log来保存数据，磁盘上一个只能追加写（Append-only）消息的物理文件，避免了随机IO操作
	日志段（Log Segment）机制：每个Partition中的日志又近一步细分成多个日志段，消息被追加写到当前最新的日志段

- Consumer Group
	 P2P模型：多个消费者实例共同组成一个组来消费一组主题，这组主题中的每个分区都只会被组内的一个消费者实例消费，提升整个消费者的吞吐量（TPS）

- Rebalance
	消费者组内某个实例挂掉了，Kafka 能够自动检测到，然后把这个消费者实例之前负责的分区转移给其他活着的消费者

- Consumer Offset
	记录消费者当前消费到了分区的哪个位置
```

Kafka只是消息引擎吗？

```markdown
- 定位：消息引擎系统 & 分布式流处理平台
- 名字来源：写的速度比较快，所以用作家名字
- Kafka Stream与flink等流处理平台相比：端到端、相对轻量
```

Kafka的版本演进

```markdown
0.7：只提供了最基础的消息队列功能
0.8：副本机制->高可靠
0.9.0.0：
	2015年11月 
	Producer相对稳定
	增加基础的安全认证和权限功能
0.10.0.0：引入了 Kafka Streams
0.10.2.2：Consumer相对稳定
0.11.0.0：
	2017年6月
	提供事务（Transaction） API
	对消息格式做了重构
0.11.0.3：国内主流、稳定
后续的大多数改进主要针对：Kafka Streams
```







## 原理篇







## 运维部署篇







- [x] 09 生产者消息分区机制原理剖析
- [x] 10 生产者压缩算法面面观



## 参考

- 《Apache Kafka源码剖析》
- 《Kafka 核心技术与实战》

> 资料来源于网络，侵删