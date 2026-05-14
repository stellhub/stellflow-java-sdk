# stellflow-java-sdk

`stellflow-java-sdk` 是 [Stellflow](https://github.com/stellhub/stellflow) 的 Java 客户端实现，用于对接 Stellflow Broker 数据面自定义二进制协议，提供 Producer、Consumer、Admin 与底层协议编解码能力。

当前 SDK 的首要目标不是复刻 Kafka Java Client 的全部实现细节，而是严格按照 Stellflow 服务端协议完成一套可演进、可测试、可跨语言对齐的 Java 客户端基线。

## 项目定位

Stellflow 服务端保留 Kafka 风格的 Topic / Partition / Replica / ISR / Offset / Consumer Group 语义，但 Broker / Client 通信使用 Stellflow 自定义二进制协议。Java SDK 负责把这些协议能力封装成易用的客户端 API：

- `StellflowProducer`：消息批量发送、分区路由、acks、超时、重试与后续幂等能力。
- `StellflowConsumer`：元数据发现、Fetch 长轮询、位点管理、消费组协调与提交位点。
- `StellflowAdminClient`：Topic、Broker、集群状态等管理接口。
- `stellflow-protocol`：协议头、请求体、响应体、错误码、API Key、RecordBatch 与兼容性测试基线。
- `stellflow-network`：基于 TCP 长连接的数据面网络层，负责帧编解码、请求关联、连接池与重连。

## 服务端协议依据

SDK 以 Stellflow 服务端仓库中的协议文档和 `network.protocol` 包作为实现依据：

- [协议规范文档](https://github.com/stellhub/stellflow/blob/main/docs/protocol-spec.md)
- [ApiVersions / Metadata 消息格式规范](https://github.com/stellhub/stellflow/blob/main/docs/api-versions-and-metadata-format.md)
- [Produce / Fetch 消息格式规范](https://github.com/stellhub/stellflow/blob/main/docs/produce-fetch-message-format.md)
- [FetchRequestBody 消息格式规范](https://github.com/stellhub/stellflow/blob/main/docs/fetch-request-format.md)
- [ProduceResponseBody 消息格式规范](https://github.com/stellhub/stellflow/blob/main/docs/produce-response-format.md)
- [ListOffsets 接口说明](https://github.com/stellhub/stellflow/blob/main/docs/list-offsets-interface.md)

协议当前基线：

- 数据面使用 TCP 长连接。
- 线上帧格式为 `frameLength + header + body`。
- 多字节整数统一使用大端序，也就是 network byte order。
- `headerVersion` 当前正式基线为 `2`。
- 每个 `apiKey` 独立维护 `apiVersion`，当前核心 API 以 `apiVersion = 0` 为第一版实现目标。
- 客户端必须先通过 `ApiVersions` 做能力协商，再发送 Metadata / Produce / Fetch 等业务请求。

## 协议帧模型

每个请求帧和响应帧都遵循同一层级：

```text
+----------------+----------------+----------------+
| frameLength    | header         | body           |
+----------------+----------------+----------------+
```

基础编码规则：

| 类型 | 编码 |
| --- | --- |
| `int8` | 1 字节有符号整数 |
| `int16` | 2 字节，大端序 |
| `int32` | 4 字节，大端序 |
| `int64` | 8 字节，大端序 |
| `bool` | 1 字节，`0` 或 `1` |
| `string` | `int16 length + UTF-8 bytes` |
| `nullable string` | `int16 length`，`-1` 表示 `null` |
| `bytes` / `nullable bytes` | `int32 length + raw bytes`，`-1` 表示 `null` |
| `array<T>` | `int32 length + repeated entries` |

请求头字段顺序固定为：

```text
apiKey
-> apiVersion
-> headerVersion
-> correlationId
-> clientId
-> traceId
-> spanId
-> traceFlags
-> tenantId
-> quotaKey
-> authContextId
-> trafficClass
-> trafficTag
-> flags
```

响应头字段顺序固定为：

```text
correlationId
-> headerVersion
-> errorCode
-> throttleTimeMs
```

客户端网络层必须维护递增或唯一的 `correlationId`，并用它把响应路由回对应的 in-flight 请求。

## API Key 范围

当前服务端对外核心 API：

| apiKey | 名称 | 用途 |
| --- | --- | --- |
| `0` | `ApiVersions` | 查询 Broker 支持的 API 版本范围和特性 |
| `1` | `Metadata` | 查询 Broker、Topic、Partition、Leader、Replica、ISR 元数据 |
| `2` | `Produce` | 写入 RecordBatchSet |
| `3` | `Fetch` | 拉取 RecordBatchSet，普通消费和副本复制复用该语义 |
| `4` | `ListOffsets` | 查询 earliest / latest / timestamp 对应 offset |
| `5` | `OffsetCommit` | 提交消费组位点 |
| `6` | `OffsetFetch` | 查询消费组位点 |
| `7` | `FindCoordinator` | 查找消费组协调器 |
| `8` | `Heartbeat` | 消费组心跳 |
| `9` | `JoinGroup` | 加入消费组 |
| `10` | `SyncGroup` | 同步分区分配 |

当前服务端实现中还预留了管理类 API：

| apiKey | 名称 |
| --- | --- |
| `50` | `CreateTopic` |
| `51` | `DeleteTopic` |
| `52` | `AlterPartition` |
| `53` | `DescribeCluster` |
| `54` | `HealthCheck` |
| `55` | `DecommissionBroker` |

SDK 实现时应把 API Key、请求体 codec、响应体 codec 放在统一注册表中管理，避免上层客户端直接依赖数字常量。

## 客户端架构

建议按以下分层实现：

```text
public api
  -> producer / consumer / admin
  -> metadata manager
  -> protocol client
  -> connection pool
  -> frame codec
  -> tcp transport
```

### 1. Public API 层

对外暴露稳定的 Java API，隐藏协议细节：

- Producer 返回 `CompletableFuture<RecordMetadata>` 或同步封装。
- Consumer 暴露 `poll`、`commitSync`、`commitAsync`、`seek`、`subscribe` 等基础能力。
- AdminClient 暴露 topic 管理、集群状态、健康检查等接口。

Public API 不直接处理 ByteBuf、帧长度、headerVersion 和 API Key 数字值。

### 2. Metadata 层

Metadata 是客户端路由事实来源：

- `bootstrap.servers` 只用于初次连接。
- 启动后先请求 `ApiVersions`，再请求 `Metadata`。
- Producer 根据 `MetadataResponse` 中的 partition leader 路由写入。
- Consumer 根据分区 leader 路由 Fetch。
- 当收到 `NOT_LEADER_OR_FOLLOWER`、`LEADER_NOT_AVAILABLE`、`BROKER_NOT_AVAILABLE`、`UNSUPPORTED_VERSION` 等错误时刷新 metadata 或能力缓存。

Metadata 缓存建议至少包含：

- `clusterId`
- brokerId 到 endpoint 的映射
- topic / partition 到 leader broker 的映射
- partition leaderEpoch、replicas、ISR
- broker endpoint 到 API version 范围的能力缓存

### 3. Protocol Client 层

Protocol Client 是 SDK 的核心请求执行器：

- 统一构造 `RequestHeader`。
- 按 `apiKey + apiVersion` 查找 body codec。
- 写入 `frameLength`、header 与 body。
- 维护 in-flight 请求表。
- 解码响应头，校验 `correlationId`。
- 把顶层 `errorCode` 与分区级 `errorCode` 映射为 SDK 异常或结果对象。

能力协商流程：

1. 连接任意 bootstrap broker。
2. 发送 `ApiVersionsRequest`。
3. 读取每个 API 的 `minVersion / maxVersion`。
4. 在 SDK 支持版本与 Broker 支持版本的交集中选择最高可用版本。
5. 后续请求使用协商后的版本。
6. Broker 切换或收到 `UNSUPPORTED_VERSION` 时重新协商。

### 4. Network 层

Java SDK 建议使用 Netty 作为 TCP 传输实现，与服务端 Java 数据面保持一致：

- 每个 Broker endpoint 维护可复用长连接。
- 一个连接允许多个 in-flight 请求，通过 `correlationId` 关联响应。
- 读写超时、连接断开、Broker 不可用需要反馈给上层重试策略。
- 响应可以乱序返回，客户端不能假设先发先回。
- 连接级上下文可缓存认证、限流、能力协商与 trace 信息。

### 5. Observability 层

SDK 核心包不引入任何 Spring / Spring Boot 依赖。可观测性通过 JDK 日志和 OpenTelemetry API 暴露：

- 日志使用 `System.Logger`，由宿主应用桥接到 JUL、Logback、Log4j2 或 Stellflux 日志体系。
- 指标使用 `io.opentelemetry:opentelemetry-api`，默认绑定 `GlobalOpenTelemetry.getOrNoop()`。
- 当宿主未配置 OpenTelemetry SDK / Exporter 时，指标记录为 no-op，不影响普通客户端使用。
- Stellflux 或独立 Spring Boot starter 后续可以提供自动装配，把框架内的 `OpenTelemetry` 实例包装成 `StellflowObservability` 后传给 SDK。

当前指标覆盖：

| 指标 | 类型 | 含义 |
| --- | --- | --- |
| `stellflow.client.requests` | Counter | 协议请求总数 |
| `stellflow.client.request.errors` | Counter | 协议请求错误总数 |
| `stellflow.client.request.duration` | Histogram | 协议请求耗时，单位 ms |
| `stellflow.client.requests.inflight` | UpDownCounter | 当前 in-flight 请求数 |
| `stellflow.client.connections` | UpDownCounter | 当前活跃 TCP 连接数 |
| `stellflow.producer.records` | Counter | Producer 成功写入 record 数 |
| `stellflow.consumer.records` | Counter | Consumer 成功拉取 record 数 |
| `stellflow.consumer.offset.commits` | Counter | OffsetCommit 成功次数 |
| `stellflow.consumer.group.operations` | Counter | Join / Sync / Heartbeat 操作次数 |

### 6. Client Factory 层

SDK core 提供 `StellflowClientOptions` 和 `StellflowClientFactory` 作为可装配入口。它们只依赖 Java、Netty 和 OpenTelemetry API，不依赖 Spring：

```java
StellflowClientOptions options =
    StellflowClientOptions.builder("127.0.0.1:9092")
        .clientId("orders-service")
        .consumerOptions(StellflowConsumerOptions.defaults("orders-group"))
        .build();

try (StellflowClientFactory factory = StellflowClientFactory.create(options)) {
  StellflowProducer producer = factory.createProducer();
  StellflowConsumer consumer = factory.createConsumer();

  consumer.subscribe(List.of("orders")).join();
  List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(5)).join();
  consumer.commitSync(Duration.ofSeconds(5));
}
```

后续 Stellflux 或 Spring Boot starter 的自动装配层应只负责把外部配置绑定成 `StellflowClientOptions`，再暴露 `StellflowClientFactory`、`StellflowProducer` 和 `StellflowConsumer`，不要把 Spring 类型下沉到 SDK core。

### 7. Codec 层

Codec 层必须保持纯协议语义，不依赖 Producer、Consumer 或 Admin 的业务对象：

- `HeaderCodec`
- `ProtocolSerde`
- `ProtocolCodecRegistry`
- `RequestBodyCodec<T>`
- `ResponseBodyCodec<T>`
- `RecordBatchCodec`
- `ApiKey`
- `ErrorCode`

这样做的好处是：

- 可以独立做 golden file 测试。
- 可以和 Go SDK、服务端协议样例做跨语言兼容测试。
- 上层 API 调整不会污染 wire protocol。

## Producer 实现要点

Producer 以 RecordBatch 为一等传输单位：

- 上层单条消息先进入 accumulator。
- 按 topic / partition 聚合成 RecordBatchSet。
- `ProduceRequestBody.records` 传输的是连续 `RecordBatch` 原始字节，不是 JSON 或逐条 record 对象。
- 普通非幂等写入可使用 `producerId = -1`、`producerEpoch = -1`、`baseSequence = -1`。
- `acks` 支持 `0`、`1`、`-1`。
- 分区级返回以 `ProducePartitionResponse.errorCode` 为准，允许同一请求部分成功、部分失败。

Producer 重试时必须区分：

- 可刷新元数据后重试：`NOT_LEADER_OR_FOLLOWER`、`LEADER_NOT_AVAILABLE`。
- 可按超时策略重试：`BROKER_NOT_AVAILABLE`、网络断开。
- 不应盲目重试：`MESSAGE_TOO_LARGE`、`INVALID_RECORD`、`AUTHORIZATION_FAILED`。

后续幂等和事务能力应基于服务端协议中的 `producerId`、`producerEpoch`、`baseSequence`、`transactionalId` 与事务错误码扩展。

## Consumer 实现要点

Consumer 的读取主链路为：

1. 通过 `Metadata` 获取 topic / partition leader。
2. 通过 `ListOffsets` 解析 earliest、latest 或 timestamp 起始位点。
3. 通过 `Fetch` 按分区批量拉取 RecordBatchSet。
4. 解码 batch，按 `highWatermark` 或 `lastStableOffset` 控制可见性。
5. 使用 `OffsetCommit` / `OffsetFetch` 管理消费组位点。

Fetch 请求关键字段：

- 普通 Consumer 使用 `replicaId = -1`。
- `maxWaitMs`、`minBytes`、`maxBytes` 控制长轮询与吞吐。
- `isolationLevel = 0` 表示 `read_uncommitted`，`1` 表示 `read_committed`。
- `fetchOffset` 是当前分区起始拉取位点。
- `partitionMaxBytes` 限制单分区返回窗口。

消费组协调基础流程：

1. `FindCoordinator`
2. `JoinGroup`
3. `SyncGroup`
4. 周期性 `Heartbeat`
5. `OffsetCommit` / `OffsetFetch`

当前 Consumer 高层封装已经提供：

- `subscribe(topics)`：基于 `Metadata` 发现订阅 topic 的分区，执行 `JoinGroup` / `SyncGroup`，并启动后台 heartbeat loop。
- `poll(timeout)`：按当前 assignment 拉取消息，成功解码后推进本地 next offset。
- `commitAsync()` / `commitSync(timeout)`：提交本轮已消费到的 offset，也就是最后一条已返回消息的 `offset + 1`。
- `assign(partitions)`：用于本地测试或手动分区消费，不加入消费组，也不会启动 heartbeat。
- `StellflowConsumerOptions`：纯 Java 配置对象，可被 Stellflux 或后续 Spring Boot starter 绑定，但 SDK core 不依赖 Spring。

现阶段 `SyncGroup` 服务端协议尚未携带真实分区分配 payload，因此 SDK 在 `subscribe` 后使用 Metadata 返回的全部分区作为本地 assignment。等服务端补齐 group assignment 编码后，这里应切换为 coordinator 返回的正式分配结果。

## Admin 实现要点

AdminClient 应复用同一套协议网络层和能力协商缓存，重点封装：

- Topic 创建、删除、分区调整。
- 集群状态查询。
- Broker 健康检查。
- Broker 下线或迁移管理。

管理请求仍然走数据面二进制协议，不属于 Controller / Broker 控制面的 gRPC 链路。

## 错误处理模型

Stellflow 协议有两层错误：

- 响应头 `errorCode`：请求级错误，例如非法请求、版本不支持、认证失败。
- 响应体内分区级 `errorCode`：单 topic / partition 的局部错误，例如非 leader、未知分区、offset 越界。

SDK 不应把所有非 `NONE` 错误都简单抛成同一种异常。建议映射为：

- `StellflowException`：基础异常。
- `RetriableStellflowException`：可重试错误。
- `AuthorizationException` / `AuthenticationException`。
- `UnsupportedVersionException`。
- `UnknownTopicOrPartitionException`。
- `OffsetOutOfRangeException`。
- `RecordTooLargeException`。
- `NotLeaderOrFollowerException`。

## 可观测性与治理上下文

请求头已经为可观测性和治理预留字段，SDK 应在配置层支持：

- `clientId`
- `traceId` / `spanId` / `traceFlags`
- `tenantId`
- `quotaKey`
- `authContextId`
- `trafficClass`
- `trafficTag`
- `flags`

这些字段应该从客户端配置、请求上下文或 OpenTelemetry Context 中生成和透传。`clientId`、`tenantId`、`trafficTag` 等可能是高基数字段，默认不要直接作为全局指标标签暴露。

## 兼容性测试

SDK 的协议层应优先建立以下测试：

- Header 编解码测试。
- 基础类型大端序编解码测试。
- ApiVersions / Metadata / Produce / Fetch / ListOffsets golden file 测试。
- `correlationId` 响应匹配测试。
- 未知 `apiKey`、未知 `apiVersion`、未知 `flags` 的兼容性测试。
- 与服务端 `ProtocolSmokeTest` 或样例报文对齐的跨仓库测试。

协议层测试通过后，再推进 Producer、Consumer、Admin 的集成测试。

## 当前实现计划

推荐按以下顺序落地：

1. 协议基础包：`ApiKey`、`ErrorCode`、header、serde、codec registry。
2. Netty 网络层：frame encoder / decoder、connection、in-flight request table。
3. `ApiVersions` 与 `Metadata`：完成启动、能力协商和路由缓存。
4. Producer 最小闭环：发送 RecordBatchSet 并解析 ProduceResponse。
5. Consumer 最小闭环：ListOffsets、Fetch、RecordBatch 解码。
6. Offset 与 Group：FindCoordinator、JoinGroup、SyncGroup、Heartbeat、OffsetCommit、OffsetFetch。
7. AdminClient：topic 与 broker 管理能力。
8. 可观测性、重试策略、连接池参数与兼容性测试补齐。

## 开发要求

- JDK：当前 `pom.xml` 使用 `maven.compiler.release = 25`。
- 构建工具：Maven。
- Java 代码风格：Spring Boot 风格，优先构造器注入，必要时使用 Lombok。
- 协议代码必须避免伪实现，所有 codec 都需要对应单元测试。
- 修改协议字段时必须同步更新 README、协议测试与服务端协议文档引用。

## ApiVersions 冒烟验证

本地 Broker 默认数据面端口为 `9092`。启动 Broker 后，可运行：

```powershell
mvn -q exec:java "-Dexec.mainClass=io.github.stellhub.stellflow.sdk.tools.ApiVersionsSmokeClient" "-Dexec.args=127.0.0.1:9092"
```

也可以使用配置风格 endpoint：

```powershell
mvn -q exec:java "-Dexec.mainClass=io.github.stellhub.stellflow.sdk.tools.ApiVersionsSmokeClient" "-Dexec.args=stellflow://127.0.0.1:9092"
```

该命令会发送 `ApiVersionsRequestBody`，并打印响应头、Broker 软件信息、支持特性和 API 版本范围。
