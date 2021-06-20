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

- 目前Kafka有哪些主流的监控框架？

``` markdown
- JMXTool  社区自带

- Kafka Manager 雅虎，已改名Cluster Manager for Apache Kafka

- Burrow LinkedIn，专门监控消费者进度

- JMXTrans + InfluxDB + Grafana   还可以同时监控其他组件

- Confluent Control Center 付费

- Kafka Eagle

- Logi-KafkaManager 滴滴
```



## 原理篇（一） 生产者

- 为什么分区？

```markdown
- 提供负载均衡的能力，实现系统的高伸缩性（Scalability）
	- 主题下的每条消息只会保存在某一个分区中，不同的分区能够被放置到不同节点的机器上
	- 数据的读写操作也是针对分区这个粒度而进行的
	- 添加新的节点机器可以增加吞吐量
    
- 利用分区也可以实现其他一些业务需求，比如实现业务级别的消息顺序问题 
```

- Kafka都有哪些分区策略？

```markdown
- 分区策略是决定生产者将消息发送到哪个分区的算法,避免造成数据“倾斜”

- Kafka提供了默认的分区策略，同时也支持自定义的分区策略
	- 自定义分区策略（限制配置生产者端的参数 partitioner.class,编写一个具体的类实现org.apache.kafka.clients.producer.Partitioner接口）：基于地理分区
    - 轮询策略（Round-robin）
    - 随机策略（Randomness）
    - 按消息键保序策略（Key-ordering）：保证同一个 Key 的所有消息都进入到相同的分区里

- 默认分区策略：如果指定了 Key，那么默认实现按消息键保序策略；如果没有指定 Key，则使用轮询策略。
```

- Kafka的消息格式？(toto)

```markdown
- v1：message & message set
    - 每一个消息都会有CRC校验
    
- v2：record item & reocrd batch
	- 0.11.0.0引入
	- 把消息的公共部分抽取出来放到外层消息集合里面（去冗余）
```

![image-20210606155543513](pic\message_v1.png)

- 何时压缩？

```markdown
- 生产者端：通过compression.type制定压缩算法

- Broker
	- 大部分情况下 ,接收到消息之后是原封不动保存的
	- 两种例外重新压缩消息
		Broker端指定了和Producer端不同的压缩算法（Broker 端也有一个参数叫 compression.type。默认值是 producer）
		Broker端发生了消息格式转换（兼容，丧失了zero copy）

- CPU资源需要很充足，带宽有限也建议压缩
```

- 何时解压缩？

```markdown
- 消费者端
	- 生产者或者Broker会将启用了哪种压缩算法封装进消息集合中
	
- Broker
	- 每个压缩过的消息集合在Broker端写入时都要发生解压缩操作，为了对消息执行各种验证
	- 在写入broker时进行解压缩，对zero copy应该没有影响
```

- Kafka支持哪些压缩算法？

```markdown
- 评判标准
吞吐量：LZ4 > Snappy > zstd 和 GZIP
压缩比：zstd > LZ4 > GZIP > Snappy

- 2.1.0之后才支持 Zstandard(zstd)
```

- Kafka为何基于TCP连接？

```markdown
- 能够利用TCP的某些高级功能
	- 多路复用：在一条物理连接上创建若干个虚拟连接
	
- 目前已知的 HTTP 库在很多编程语言中都略显简陋
```

- 生产者何时创建TCP连接？

```markdown
- 获取元数据
	- 在Producer实例启动时，Producer会在后台创建并启动一个Sender线程，该线程开始运行时会创建与Broker的连接
	- 不调用send方法，这个producer不知道给哪个主题发消息，所以sender会先连接boostrap.servers参数指定的所有Broker（配置几台即可）
	- 一旦连接到集群中的任一一台Broker，会发送一个 METADATA请求，就能拿到整个集群的元数据
	
- 更新元数据
	- Producer 通过 metadata.max.age.ms 参数定期地去更新元数据信息（默认五分钟）

- 在消息发送时
	- 当要发送消息时，Producer发现尚不存在与目标Broker的连接，boostrap与Broker不是同一个
```

- 生产者何时关闭TCP连接？

```markdown
- 用户主动关闭
	- kill -9
	- producer.close()

- 自动关闭（TCP连接是在Broker端被关闭的，但其实这个TCP连接的发起方是客户端，所以其实也是被动关闭）
	- Producer 端参数 connections.max.idle.ms
		- 默认9分钟，如果在 9 分钟内没有任何请求“流过”某个 TCP 连接，那么 Kafka 会主动把该 TCP 连接关闭
		- -1  只是软件层面的“长连接”机制，还是会准守底层Socket的keepalive探活机制
```

- 消息交付可靠性保障的级别有哪些？

```markdown
- 最多一次（at most once）
	- 消息可能会丢失，但绝不会被重复发送。让 Producer 禁止重试。
	
- 至少一次（at least once）
	- 消息不会丢失，但有可能被重复发送。（默认）
	- 通过参数设置，可以保证
	- 只有Broker成功接收消息且Producer接到Broker的应答才会认为该消息成功发送
	  倘若消息成功接收到，但 Broker 的应答没有成功发送回 Producer 端（比如网络出现瞬时抖动）
	  那么 Producer 就无法确定消息是否真的提交成功了
	  因此，Producer只能选择重试，再次发送相同的消息，这就会导致消息重复发送

- 精确一次（exactly once）
	- 消息不会丢失，也不会被重复发送
```

- 什么是幂等性？

```markdown
- 操作或函数能够被执行多次，但每次得到的结果都是不变的，比如让数字乘以1
 
- 优点可以安全地重试任何幂等性操作，不会破坏系统状态
```

- 什么是幂等Producer?

```markdown
- 0.11.0.0 版本引入 props.put(“enable.idempotence”, ture) 或props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG， true)

- 原理（自动消息去重）
	- 用空间去换时间的优化思路，即在Broker端占用内存，多保存一些字段
	- 当 Producer 发送了具有相同字段值的消息后，Broker 能够自动知晓这些消息已经重复了，于是可以在后台默默地把它们“丢弃”掉

- 限制
	- 只能保证单分区上的幂等性。一个幂等性 Producer 能够保证某个主题的一个分区上不出现重复消息（todo，轮询难道不应该发送消息到多个分区吗？）
	- 只能实现单会话上的幂等性，不能实现跨会话的幂等性。
```

- 什么是事务Rroducer?

```markdown
- 事务 （自 0.11 版本开始也提供了对事务的支持），开启 enable.idempotence = true

- 在 read committed 隔离级别上做事情。当读取数据库时，你只能看到已提交的数据，即无脏读。同时，当写入数据库时，你也只能覆盖掉已提交的数据，即无脏写

- 保证Producer多条消息原子性地写入到目标分区，同时也能保证 Consumer 只能看到事务成功提交的消息（Consumer相应设置isolation.level为read_committed）

- 保证跨分区、跨会话间的幂等性
```



---------草稿

## 原理篇（二） 消费者

- 什么是消费者组？

```markdown
- 传统的“消息队列”的问题
	- 点对点模式，消息只能被下游的一个consumer消费(伸缩性差)
	- 发布 / 订阅模型，因为每个订阅者都必须要订阅主题的所有分区

- Consumer Group 是 Kafka 提供的可扩展且具有容错性的消费者机制
	- 多个消费者实例共享一个公共Group ID，组内的所有消费者协调在一起来消费订阅主题的所有分区。
	- 每个分区只能由同一个消费者组内的一个Consumer实例来消费。每个实例不要求一定要订阅主题的所有分区，它只会消费部分分区中的消息。
```

- 怎么管理消费位移？

```markdown
- 消费者在消费的过程中需要记录自己消费的位置信息
	- 对于 Consumer Group 而言，它是一组 KV 对，Key 是分区，V 对应 Consumer 消费该分区的最新位移。
	
- 老版本：位移保存在 ZooKeeper 中，将服务器节点做成无状态的（提高伸缩性），但ZooKeeper 不适合进行频繁的写更新

- 新版本：将位移保存在 Broker 端内部主题__consumer_offsets中
```

- 什么是rebalance?

```markdown
- 协议，规定了一个 Consumer Group 下的所有 Consumer 如何达成一致，来分配订阅 Topic 的每个分区
- 在 Rebalance 过程中，所有 Consumer 实例都会停止消费，等待 Rebalance 完成。类似JVM垃圾回收机制的万物静止的收集方式（stop the world）
```

- 何时进行rebalance?

```markdown
- 组成员数发生变更。新Consumer加入组或者离开组（或者有consumer崩溃被踢出组）

- 订阅主题数发生变更。Consumer Group可以使用正则表达式的方式订阅主题，在 Consumer Group的运行过程中，新创建了一个满足这样条件的主题

- 订阅主题的分区数发生变更
```

- 怎样进行rebalance?

```markdown
- 三种分配策略
	- Range：对同一个主题里面的分区按照序号进行排序。用分区数除以消费者线程数量来判断每个消费者线程消费几个分区。
	- RoundRobin
	- Sticky：0.11.X，分配要尽可能的均匀，分配尽可能的与上次分配的保持相同

- 目前 Rebalance 的设计是所有 Consumer 实例共同参与，全部重新分配所有分区。
	- 最有效的做法还是尽量减少分配方案的变动
```

- Rebalance的弊端？

```markdown
- 在整个rebalance的过程中，STW让所有实例都不能消费任何信息，对Consumer的TPS影响很大
	- 能不能参照JVM中的Minor gc和 Major gc
	- 有些分区就还是由原消费者消费，特别是针对已有主题分区数量变化

- Relalance很慢，Group下的Consumer会很多

- Rebalance 效率不高，Group下的所有成员都需要参与进来，而且通常不会考虑局部性原理
```

- rebalance是如何通知到其他消费者实例的？

```markdown
- 靠消费者端的心跳线程
	- 重平衡的通知机制：Coordinator会将“REBALANCE_IN_PROGRESS”封装进心跳请求的响应中，发还给消费者。当消费者发现心跳响应中包含了“REBALANCE_IN_PROGRESS”，就能立马知道重平衡又开始了
	
- consumer需要的定期的发送心跳到Coordinator
	- 0.10.1.0之前，由消费者主线程完成，也就是写代码调用 KafkaConsumer.poll 方法的那个线程，但是消息处理逻辑也是在这个线程中完成的。因此，一旦消息处理消耗了过长的时间，心跳请求将无法及时发到Coordinator那里，导致Coordinator“错误地”认为该消费者已“死”。
	- 0.10.1.0开始，一个单独的心跳线程来专门执行心跳请求发送
```

- 什么是消费者组状态机（消费者组的状态）？

```markdown
- Empty：组内没有任何成员，但是消费者组可能存在已提交的位移数据或元数据
- Dead：组内没有任何成员，并且元数据信息已经在Coordinator端移除
- PreparingRebalance：消费者组准备开启rebalance,此时所有成员都要重新请求加入消费者组
- CompletingRebalance：消费者组下所有成员已经加入，各个成员正在等待分配方案（老版本叫AwaitingSync）
- Stable：消费者组的稳定状态，表明已经完成rebalance

一个消费者组最开始是 Empty 状态，当重平衡过程开启后，它会被置于 PreparingRebalance 状态等待成员加入，之后变更到 CompletingRebalance 状态等待分配方案，最后流转到 Stable 状态完成重平衡

当有新成员加入或已有成员退出时，消费者组的状态从 Stable 直接跳到 PreparingRebalance 状态，此时，所有现存成员就必须重新申请加入组

当所有成员都退出组后，消费者组状态变更为 Empty。Kafka 定期自动删除过期位移的条件就是，组要处于 Empty 状态。
```

<img src="pic\consumer state.png" alt="image-20210619184436035" style="zoom:50%;" />

- 消费者组进行rebalance的流程？

```markdown
- rebalance的完整流程需要消费者端和协调者组件共同参与才能完成

- 消费者端
	- 加入组：消费者组的成员会向协调者发送 JoinGroup 请求。在该请求中，每个成员都要将自己订阅的主题上报，这样协调者就能收集到所有成员的订阅信息，一旦收集了全部成员的 JoinGroup 请求后，协调者会从这些成员中选择一个担任这个消费者组的领导者
		- 通常情况下，第一个发送 JoinGroup 请求的成员自动成为领导者
		- 领导者消费者的任务是收集所有成员的订阅信息，然后根据这些信息，制定具体的分区消费分配方案
		- 选出领导者之后，协调者会把消费者组订阅信息封装进 JoinGroup 请求的响应体中，然后发给领导者，由领导者统一做出分配方案
	- 等待领导者消费者（Leader Consumer）分配方案
		- 领导者向协调者发送 SyncGroup 请求，将刚刚做出的分配方案发给协调者，其他成员也会向协调者发送 SyncGroup 请求，只不过请求体中并没有实际的内容
		- 协调者接收分配方案，然后统一以 SyncGroup 响应的方式分发给所有成员，当所有成员都成功接收到分配方案后，消费者组进入到 Stable 状态，即开始正常的消费工作
```

- Coordinator配合Rebalance的场景和流程？

```markdown
- 场景一：新成员入组
	当协调者收到新的 JoinGroup 请求后，它会通过心跳请求响应的方式通知组内现有的所有成员，强制它们开启新一轮的重平衡。

- 场景二：组成员主动离组
	消费者实例所在线程或进程调用 close() 主动通知协调者它要退出。协调者收到 LeaveGroup 请求后，依然会以心跳响应的方式通知其他成员准备开始rebalance

- 场景三：组成员崩溃离组
	消费者实例出现严重故障，突然宕机导致的离组。被动的，协调者通常需要等待一段时间才能感知到，这段时间一般是由消费者端参数 session.timeout.ms 控制的

- 场景四：rebalance时协调者对组内成员提交位移的处理
	每个组内成员都会定期汇报位移给协调者。当rebalance开启时，协调者会给予成员一段缓冲时间，要求每个成员必须在这段时间内快速地上报自己的位移信息，然后再开启正常的 JoinGroup/SyncGroup 请求发送
```

- 什么是Coordinator?

```markdown
- 专门为 Consumer Group 服务，负责为 Group 执行 Rebalance 以及提供位移管理和组成员管理等
	Consumer提交位移时，其实是向Coordinator所在的Broker提交位移。
  	Consumer启动时，也是向Coordinator所在的Broker发送各种请求，然后由Coordinator负责执行消费者组的注册、成员管理记录等元数据管理操作
  
- 所有 Broker 都会创建和开启相应的Coordinator组件
```

- Consumer Group 如何确定为它服务的 Coordinator 在哪台 Broker 上呢？

```markdown
1 先确定由位移主题的哪个分区来保存该Group数据
	partitionId=Math.abs(groupId.hashCode() % offsetsTopicPartitionCount)
2 找出该分区Leader副本所在的Broker，该Broker即为对应的Coordinator
```

- 如何避免rebalance？

```markdown
- rebalance的时机
	组成员数量发生变化
	订阅主题数量发生变化 (运维主动操作)
	订阅主题的分区数发生变化 (运维主动操作)

- 组成员数量发生变化
	- 未能及时发送心跳，导致 Consumer 被“踢出”Group 而引发的
		让其至少能够发送至少 3 轮的心跳请求
		session.timeout.ms=6s
		heartbeat.interval.ms = 2s

	- Consumer 消费时间过长导致的
		- 如果在每次拉取消息之后需要对消息做一定的处理
			需要把max.poll.interval.ms设置的稍微大一些，预估一下处理消息的最长时间，给业务留下必要的时间(Consumer消费能力过低或者一次性拉取数据过多)
		- 排查一下Consumer端的GC表现，比如是否因为频繁的Full GC导致Consumer长时间的停顿，而引发非预期的Rebalance
```

- 如何得知broker rebalance过多？

```markdown
- Coordinator所在的broker日志，如果经常发生rebalance，会有类似于"(Re)join group" 之类的日志
```

- 什么是位移主题（__consumer_offsets）?

```markdown
- 老版本：保存在ZooKeeper中，减少Broker的状态信息，高伸缩性，但ZooKeeper 并不适用于这种高频的写操作

- 新版本：将位移数据作为一条条普通的 Kafka 消息，提交到 __consumer_offsets 中，刚好Kafka自身能够实现高持久性和高频的写操作

	- 为什么不用HashMap存一次然后替换值就行？ 保证顺序IO，高吞吐
```

- 什么是位移主题有哪些信息?

```markdown
- 消息格式是Kafka自定义的，用户不能随意修改

- 三类消息
	保存位移的消息:  key = <Group ID，主题名，分区号>  value = 位移 & 位移相关的元数据
	保存Consumer Group信息的消息，用来注册Consumer Group
	删除Group过期位移、删除Group的消息
```

- 什么时候创建位移主题？

```markdown
- 当 Kafka 集群中的第一个 Consumer 程序启动时，就会会自动创建位移主题
	- 分区数：Broker 端参数 offsets.topic.num.partitions，通常来说，同一个group下的所有消费者提交的位移数据保存在同一个分区中
	- 副本数：offsets.topic.replication.factor

- 也可以在Kafka集群尚未启动任何Consumer之前手动创建
```

- 什么时候向位移主题写入位移信息呢？

```markdown
- 自动提交位移：只要Consumer一直启动，就会无限期地向位移主题写入信息，这就需要重复消息删除策略
	enable.auto.commit = true
	auto.commit.interval.ms

- 手动提交位移
	enable.auto.commit = false
```

- 怎样删除位移主题中的过期消息？

```markdown
- Compaction（整理机制）：
	对于同一个 Key 的两条消息 M1 和 M2，如果 M1 的发送时间早于 M2，那么 M1 就是过期消息。
	Compact 的过程就是扫描日志的所有消息，剔除那些过期的消息，然后把剩下的消息整理在一起。

- 专门的后台线程Log Clean定期地巡检待Compact的主题
  	如果位移主题无限膨胀占用很多磁盘空间，很有可能就是这个线程挂了
```

- consumer位移提交的分类？

```markdown
- 用户角度
	- 自动提交
		enable.auto.commit=true
		auto.commit.interval.ms
		poll方法会先提交上一批消息的位移，再处理下一批消息，因此能保证不出现消费丢失的情况，但是会出现消费重复的情况
	
	- 手动提交
		KafkaConsumer#commitSync() 同步
		KafkaConsumer#commitAsync() 异步
		消费者 poll 方法内部维护一个不可见的指针，commitAysnc 方法异步提交不管是否成功，poll 仍然能根据自己维护的指针位移消费数据，最后在finally内用同步方法， 同步最新的位移。		
		结合异步&同步：对于常规性、阶段性的手动提交，调用 commitAsync() 避免程序阻塞，而在 Consumer 要关闭前，调用 commitSync() 方法执行同步阻塞式的位移提交，以确保 Consumer 关闭前能够保存正确的位移数据。

- Consumer角度：同步提交&异步提交
```

- 什么是细粒度的位移提交？

```markdown
- poll方法返回很多消息，肯定不希望所有消息都处理完之后再提交，一旦出现差错，之前处理的全部都要再重来一遍
	类似于数据库中的事务处理，希望将大事务分隔为若干个小事务分别提交

- 手动消费一定位移就提交一次
```

- 为什么手动提交位移也不能避免消息重复消费？

```markdown
- 假设 Consumer 在处理完消息和提交位移前这段时间出现故障，下次重启后依然会出现消息重复消费的情况

- 业务中怎么进行去重呢？
  主要还是依赖于业务代码
  设置特别的业务字段，用于标识消息的id，再次遇到相同id则直接确认消息
  将业务逻辑设计为幂等，即使发生重复消费，也能保证一致性
```

- 什么是CommitFailedException？怎样防止发生？

```markdown
- 手动提交位移时KafkaConsumer.commitSync()，出现了不可恢复的严重异常
  消息处理的总时间超过预设的 max.poll.interval.ms 参数值时会发生
  
- 处理办法
	- 缩短单条消息处理的时间，优化下游的逻辑是最不容易出错的
	- 权衡TPS和消费延时
		增加consumer消费下一批消息的时长间隔 max.poll.interval.ms
		减少一次性消费信息的总数 max.poll.records
	- 下游系统使用多线程来加速消费
		让下游系统手动创建多个消费线程处理 poll 方法返回的一批消息
		但是多个线程在如何处理位移提交上容易出错
```

- Java Consumer是怎样设计的？

```markdown
- 从 Kafka 0.10.1.0 版本开始，KafkaConsumer 就变为了双线程的设计
	- 用户主线程：处理消息的逻辑是否采用多线程，完全由自己决定
	- 心跳线程：负责定期检查主线程的阻塞情况，然后给协调者发送心跳请求（解耦）
```

- 多线程消费者的设计方案？   （**细化为实战重点**）

> Java KafkaConsumer不是线程安全的

```java
方案一： 多线程 + 多KafkaCOnsumer实例
    消费者程序启动多个线程，每个线程维护专属的 KafkaConsumer 实例，负责完整的消息获取、消息处理流程
    优点：方便实现；速度快，五线程间交互开销；易于维护分区内的消费顺序
    缺点：占用更多的系统资源；线程数受限于主题的分区数，拓展性差；线程自己处理消息很容易超时，从而引发rebalance
    
public class KafkaConsumerRunner implements Runnable {
     private final AtomicBoolean closed = new AtomicBoolean(false);
     private final KafkaConsumer consumer; // 这里的伪代码有些问题，没有创建对象啊

     public void run() {
         try {
             consumer.subscribe(Arrays.asList("topic"));
             while (!closed.get()) {
      ConsumerRecords records = 
        consumer.poll(Duration.ofMillis(10000));
                 //  执行消息处理逻辑
             }
         } catch (WakeupException e) {
             // Ignore exception if closing
             if (!closed.get()) throw e;
         } finally {
             consumer.close();
         }
     }

     // Shutdown hook which can be called from a separate thread
     public void shutdown() {
         closed.set(true);
         consumer.wakeup();
     }
```

![img](pic\consumer多线程 方案1.jpg)

```java
方案二： 单线程 + 单KafkaConsumer实例 + 消息处理Worker线程池
     优点：可独立扩展消费获取线程数和Worker线程数，伸缩性好
     缺点：实现难度高，难以维护分区内的消息消费顺序；处理链路拉长，不易于位移提交管理

private final KafkaConsumer<String, String> consumer;
private ExecutorService executors;
...

private int workerNum = ...;
executors = new ThreadPoolExecutor(
  workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
  new ArrayBlockingQueue<>(1000), 
  new ThreadPoolExecutor.CallerRunsPolicy());

...
while (true)  {
  ConsumerRecords<String, String> records = 
    consumer.poll(Duration.ofSeconds(1));
  for (final ConsumerRecord record : records) {
    executors.submit(new Worker(record));
  }
}
..
```

<img src="pic\consumer多线程方案2.jpg" alt="img" style="zoom:50%;" />









## 原理篇（三） 内核

- 什么是副本？副本的好处？

```markdown
- 本质上是一个只能追加写信息的日志,每个分区配置有若干个副本

- 提供数据冗余，保证数据持久性
- 提高伸缩性，提高读的吞吐量（Kafka未提供）
- 改善数据局部性（允许将数据放入与用户地理位置相近的地方，从而降低系统延时）
```

- 为什么追随者副本不对外提供服务？

```markdown
- Kafka在分区级别已经提高了伸缩性（吞吐量）

- 为了保证 数据一致性
	方便实现“Read-your-writes”，副本同步是异步的，因此有可能出现追随者副本还没有从领导者副本那里拉取到最新的消息，从而使得客户端看不到最新写入的消息
	方便实现单调读（Monotonic Reads），不同副本的同步时间也不一样
```

- 什么是基于领导者（Leader-based）的副本机制？

```markdown
- 领导者副本（Leader Replica）
	创建Topic时选取，领导者副本所在的 Broker 宕机时，开启新一轮的领导者选举
	
- 追随者副本（Follower Replica）	
	不对外提供服务的，唯一的任务就是从领导者副本异步拉取消息，并写入到自己的日志中，从而实现与领导者副本的同步
```

<img src="pic\leader-follower.png" alt="image-20210618192721299" style="zoom: 25%;" />

- 什么是ISR?

```markdown
- In-sync Replicas（ISR）：ISR 副本集合是与 Leader 同步的副本的集合
```

- 什么副本能够进入到 ISR 中呢？

```markdown
- Leader 副本天然就在 ISR 中

- 判断 Follower 是否与 Leader 同步的标准，不是看相差的消息数, 是看Broker 端参数 replica.lag.time.max.ms 参数值,如果leader发现flower超过这个参数所设置的时间没有向它发起fech请求，那么虑将这个flower从ISR移除

- 被剔除的副本追上了Leader的进度，是可以重新被加回ISR的
```

- 什么是Unclean leader的选举？

```markdown
- 既然 ISR 是可以动态调整的，那么自然就可以出现这样的情形：ISR 为空。因为 Leader 副本天然就在 ISR 中，如果 ISR 为空了，就说明 Leader 副本也“挂掉”了，Kafka 需要重新选举一个新的 Leader。

- 通常来说，非同步副本落后 Leader 太多，如果选择这些副本作为新 Leader，就可能出现数据的丢失，因为其他副本会开始同步这个新的Leader

- Broker 端参数 unclean.leader.election.enable 控制是否允许 Unclean 领导者选举
	- 开启，可能会造成数据丢失，使得分区 Leader 副本一直存在，提高可用性
	- 禁止，维护了数据的一致性，避免了消息丢失，但牺牲了高可用性
	- 建议不要开启，为了这点儿高可用性的改善，牺牲了数据一致性，那就非常不值当了
```

- Kafka中请求的分类？

```markdown
- 所有的请求都是通过TCP以Socket的方式进行通讯的

- 数据类请求:
	PRODUCE 生产数据
	FETCH   消费数据

- 控制类请求
	LeaderAndIsr  更新 Leader 副本、Follower 副本以及 ISR 集合
	StopReplica   勒令副本下线
	
- 控制类请求可以直接令数据类请求失效。两类请求混合交替出现会有很多问题
	 2.3 版本正式实现了数据类请求和控制类请求的分离, Broker 启动后，会在后台分别创建两套网络线程池和 IO 线程池的组合，它们分别处理数据类请求和控制类请求

- 有个主题只有 1 个分区，该分区配置了两个副本，其中 Leader 副本保存在 Broker 0 上，Follower 副本保存在 Broker 1 上。假设 Broker 0 这台机器积压了很多的 PRODUCE 请求，此时你如果使用 Kafka 命令强制将该主题分区的 Leader、Follower 角色互换，那么 Kafka 内部的控制器组件（Controller）会发送 LeaderAndIsr 请求给 Broker 0，显式地告诉它，当前它不再是 Leader，而是 Follower 了，而 Broker 1 上的 Follower 副本因为被选为新的 Leader，因此停止向 Broker 0 拉取消息。这时，如果刚才积压的 PRODUCE 请求都设置了 acks=all，那么这些在 LeaderAndIsr 发送之前的请求就都无法正常完成了，它们会被暂存在 Purgatory 中不断重试，直到最终请求超时返回给客户端。
```

- 处理请求的方式？

```markdown
- 顺序处理请求：吞吐量差
- 每个请求使用单独线程处理：创建和销毁线程耗费资源

- Reactor模式

	1. 当网络线程池拿到请求后，不自己处理，而是将请求放入到一个共享请求队列中。网络线程池默认是 3
	2. IO 线程池负责从该队列中取出请求，执行真正的处理（如果是 PRODUCE 生产请求，则将消息写入到底层的磁盘日志中；如果是 FETCH 请求，则从磁盘或页缓存中读取消息）
		num.io.threads 默认值是 8
    3.	当 IO 线程处理完请求后，会将生成的响应发送到网络线程的响应队列中，然后由对应的网络线程负责将 Response 返还给客户端(请求队列是所有网络线程共享的，而响应队列则是每个网络线程专属的)
```

![image-20210618195337922](pic\请求处理.png)

- 什么是Purgatory？

```markdown
- Kafka 中著名的“炼狱”组件

- 缓存延时请求（Delayed Request，一时未满足条件不能立刻处理的请求）:比如设置了 acks=all 的 PRODUCE 请求，请求就必须等待 ISR 中所有副本都接收了消息后才能返回，此时处理该请求的 IO 线程就必须等待其他 Broker 的写入结果。

- 当请求不能立刻处理时，它就会暂存在 Purgatory 中。稍后一旦满足了完成条件，IO 线程会继续处理该请求，并将 Response 放入对应网络线程的响应队列中。
```





## 参考

- 《Apache Kafka源码剖析》
- 《Kafka 核心技术与实战》

> 资料来源于网络，侵删	