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







## 部署篇

Kafka线上部署需要考虑些什么？

```markdown
- 操作系统
	- Kafka 客户端底层使用了 Java 的 selector
        selector 在 Linux 上的实现机制是 epoll（会把哪个流发生了怎样的I/O事件通知我们）
        selector 在 Windows 平台上的实现机制是 select（有I/O事件发生却并不知道是哪几个流，只能无差别轮询）
    - Linux 部署能够享受到零拷贝技术
    
- 磁盘
	- 大多是使用顺序读写，普通机械硬盘即可，使用磁盘阵列那更好

- 磁盘容量
	- Broker 需要保存数据
	- 规划：新增消息数、消息留存时间、消息大小、备份数、是否启用压缩
		
- 带宽
	- 通过网络大量进行数据传输的框架而言，带宽特别容易成为瓶颈
	- 真正要规划的是 所需 Kafka 服务器的数量
```

- （案例）假设公司的机房环境是千兆网络，即 1Gbps。业务目标是在 1 小时内处理 1TB 的业务数据。那么需要多少台 Kafka 服务器来完成这个业务呢？

```markdown
- 带宽 1Gbps，即每秒处理 1Gb 的数据

- 假设每台Kafka所在的机器上都没有混部其他服务，通常情况下假设Kafka会用70%的宽带资源，那么每台服务器大约能使用最大带宽资源是700Mbps

- 为follower拉取留取一些带宽等，那么设置常规性带宽资源位最大资源的1/3,即单台服务器使用带宽240Mbps

- 1TB = 1024*1024*8 Mb -> 每s需处理2330Mb数据

- 则需要 2330 / 240 约等于 10台服务器

- 如果消息还需要有两个 follow副本，则服务器台数还要乘 3
```

- Kafka有哪些重要的参数（要对默认参数进行修改）？

```markdown
- Broker端参数（server.properties）
	- 存储
		log.dirs/log.dir： 没有默认值；最好配置多个路径，并且挂载到不同的物理磁盘上，以便提升读写性能和实现故障转移
	- 数据留存
		log.retention.{hours|minutes|ms}
		log.retention.bytes：默认值是 -1 表明保存多少数据都可以，应用场景应该是在云上的多租户
		message.max.bytes：控制Broker能够接受的最大消息大小
	- ZooKeeper
   		zookeeper.connect
    - 连接
		listeners：<协议名称，主机名，端口号> -> PLAINTEXT://localhost:9092,告诉外部连接者要通过什么协议访问指定主机 Kafka 服务
	- topic 
		auto.create.topics.enable：是否允许自动创建 Topic，最好为false，方便管理
		unclean.leader.election.enable：保存数据比较多的副本挂了，是否允许进度慢的副本竞选（允许的话，数据有可能丢失，不允许的话，这个分区也就不可用了），最好为false
		auto.leader.rebalance.enable：是否允许定期地对一些 Topic 进行 Leader 重选举，最好为false

- Topic级别参数（覆盖全局 Broker 参数，可以创建Topic时设置，也可以修改）
	- 消息留存
		retention.ms：默认7天
		retention.bytes：-1
		max.message.bytes

- JVM参数
	- Scala编译成的Class文件还是在JVM上运行
	- GC推荐G1

- 操作系统参数
	- 文件描述符限制：有些系统一个长连接会占用一个Socket句柄，可能经常看到 too many open files 的错误
	- 文件系统类型
	- Swappiness
		可以不要设置位0，因为一旦设置成 0，当物理内存耗尽时，操作系统会触发 OOM killer，如果设置成一个比较小的值，当开始使用 swap 空间时，至少能够观测到 Broker 性能开始出现急剧下降，从而给进一步调优和诊断问题的时间
	- 提交时间/Flush 落盘时间
```














- [x] 09 生产者消息分区机制原理剖析
- [x] 10 生产者压缩算法面面观



## 参考

- 《Apache Kafka源码剖析》
- 《Kafka 核心技术与实战》

> 资料来源于网络，侵删