# Flink: FlieSystem SQL Connector

> Flink：1.13
>
> 基于https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/filesystem/ 的翻译及理解过程。
>
> Author：周佳豪
>
> Version：1.0.0
>
> Date：2022-08-16



## 建表语句

```sql
CREATE TABLE MyUserTable (
  column_name1 INT,
  column_name2 STRING,
  ...
  part_name1 INT,
  part_name2 STRING
) PARTITIONED BY (part_name1, part_name2) WITH (
  'connector' = 'filesystem',           -- 必填: filesystem
  'path' = 'file:///path/to/whatever',  -- 必填: hdfs路径
  'format' = '...',                     -- 必填: 支持csv、json、Apache Avro、Debezium CDC、Canal CDC、Maxwell CDC、Apache Parquet、Apache ORC、Raw
  'partition.default-name' = '...',     -- 选填: 动态分区列未指定或是空字符串时的默认分区名
  'sink.shuffle-by-partition.enable' = '...',-- 选填: 开启后，在sink阶段能通过分区字段对数据进行shuffle，这可以极大的减少fliesystem sink中的文件数量，但也可能造成数据倾斜。默认关闭。
  ...
)
```

<hr>

## 分区文件

Flink文件系统的分区支持采用hive标准格式，不需要提前为分区而去注册表的catalog信息。

也就是说，根据目录结构，Flink就能自己发现并推断出分区。

文件系统表支持insert和insert overwrite两种插入方式，insert overwrite某张分区表的话，只有对应的分区会被覆盖，而不是整张表。

<hr>

## 文件格式

文件系统的连接器支持以下几种格式：

- CSV: [RFC-4180](https://tools.ietf.org/html/rfc4180). 不压缩
- JSON: 要注意，文件系统连接器的json格式不是典型的json文件，是不压缩的。
- Avro: [Apache Avro](http://avro.apache.org/). 支持通过配置 avro.codec 参数而压缩
- Parquet: [Apache Parquet](http://parquet.apache.org/). 与hive兼容
- Orc: [Apache Orc](http://orc.apache.org/). Compatible with Hive.
- Debezium-JSON: [debezium-json](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/formats/debezium/).
- Canal-JSON: [canal-json](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/formats/canal/).
- Raw: [raw](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/formats/raw/).

<hr>

## 流数据Sink

### 滚动策略<很重要>

- #### 先理解什么是part文件

  存放在分区路径下的数据会切分成多个part文件。

  > Each partition will contain at least one part file for each subtask of the sink that has received data for that partition. 
  >
  > 每个分区将包含：接收该分区数据的sink的每个子任务(产生)的至少一个part文件。

  即当前sink有多个子任务，每个子任务都在承担往sink写数据的责任，各自都会产生各自的part文件。

  接收到的数据将根据 **”滚动策略“ **形成part文件，这个策略可以是每个part文件的最大文件大小、也可以是多长时间产生一个新的part文件。

  part文件的生命周期有三个阶段[https://blog.csdn.net/haozhuxuan/article/details/122078203]：

  | 阶段        | 描述                                                         |
  | ----------- | ------------------------------------------------------------ |
  | in-progress | 数据正在写入。                                               |
  | pending     | 数据写入完成，文件处于closed状态，已不能写入。Pending 意为"待决的"，意指正在等待 Checkpoint。 |
  | finished    | 在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态，处于 Finished 状态的文件不会再被修改，可以被下游系统安全地读取。 |

- #### 再理解什么是滚动

  这里的 **“滚动”** 意指 **关闭当前在写的part文件、产生一个新的part文件** 的 **动作**。

  策略有两类：

  - 写到多大：当前part文件的大小写到一个阈值就关闭，然后开启一个新的part文件来写入新来的数据。

  - 写了多久：每隔多少时间关闭当前写的part文件，然后开启新的part文件来写入新来的数据。

- #### 配置项列表

  | 配置项                                | 默认值 | 类型     | 描述                                                         |
  | ------------------------------------- | ------ | -------- | ------------------------------------------------------------ |
  | sink.rolling-policy.file-size         | 128MB  | 内存大小 | 每个part文件最大的大小，写到阈值就关闭当前文件，开启下一个。<br />经实测，没有完全严格到字节，以设置 1M(1048576字节) 为例，文件字节数可能大于1048576字节。这个应该也是为了保证每行数据的完整性而给予的弹性。<br />**注意**：这一点将影响到文件自动合并的配置项，因为文件自动合并是严格按照字节数的(踩过坑)。 |
  | sink.rolling-policy.rollover-interval | 30 min | 间隔时间 | 当前part文件最多写多久关闭，然后新part文件开启。             |
  | sink.rolling-policy.check-interval    | 1 min  | 间隔时间 | 每多少时间检查一下 **是否** 旧part文件需关闭、新part文件需开启，与“sink.rolling-policy.rollover-interval”参数强相关。 |

 -  #### **注意点<很重要>**

    - 对于 **行存储** 格式(csv, json)文件 ，配置

      - sink.rolling-policy.file-size
      - sink.rolling-policy.rollover-interval
      
      两项，运行过程中满足其中一项就产生part文件，然后等到checkpoint时，转为finished状态。
      
    - 对于 **列存储** 格式(parquet, avro, orc)，只需设置execution.checkpointing.interval，即在执行snapshotState方法时滚动文件，rolling-policy的两个配置项**实测**不生效。
    
      不生效的原因是，如果基于大小或者时间滚动文件，那么在任务失败恢复时就必须对处于in-processing状态的文件按照指定的offset进行truncate，由于列式存储是无法针对文件offset进行truncate的，因此就必须在每次checkpoint使文件滚动，其使用的滚动策略实现是OnCheckpointRollingPolicy。
      
      [https://cloud.tencent.com/developer/article/1887152]
    
    - hdfs配额的限制(实验中遇到的问题)
    
      hdfs路径下的**文件夹与文件数量**是有限制的，当出现以下提示：
    
      `The NameSpace quota(directories and files) of directory /../../xx.db is exceed:quota=100000 file count=10001`
    
      说明文件夹数量和文件数超出配额(这里是100000，可通过hdfs的配置文件配置)，所以在滚动策略上，需要注意生成文件的速度，不能在不加定期清理的情况下过快的生成文件。
    
      **特别是** 列式存储文件，控制文件生成速度的参数是checkpoint间隔。
    
      如果checkpoint间隔内，出现了多个文件，可以开启文件合并功能，具体内容见下文。

<hr>

### 文件合并

[https://issues.apache.org/jira/browse/FLINK-19345]

> The file sink supports file compactions, which allows applications to have smaller checkpoint intervals without generating a large number of files.
>
> 文件(系统)sink支持文件合并，即允许应用有更短的checkpoint间隔而不会生成大量文件。

文件合并的机制是：checkpoint间隔内产生的文件合并为不大于 **compaction.file-size** 的n个文件。

结合之前章节内容可知，**列式存储文件不受此配置项制约。**

| 配置项               | 默认值 | 类型       | 描述                                                         |
| -------------------- | ------ | ---------- | ------------------------------------------------------------ |
| auto-compaction      | false  | Boolean    | 是否在流数据Sink中配置自动聚合的配置项，默认关闭。           |
| compaction.file-size | (none) | MemorySize | 文件聚合后的**结果文件**的最大阈值。<br />假如设置为2M，则如果一个checkpoint内有1M、800K、1M三个文件，则合并后产生1.8M、1M两个文件。<br /><br />结合源码和实验结果，这个参数将**严格换算成字节数**。<br /> |

需要注意的是：

- 在一个checkpoint中的文件会聚合，所以在这个checkpoint阶段中，最后生成的文件数 **至少** 会与 checkpoint 数相同。
- 文件在合并前是不可见的，所以这些文件要在 **checkpoint间隔+聚合文件花费的时间** 后才可见(这句话中的**可见**应该是针对下游来说)。
- 如果聚合花了很长时间，job将出现反压情况，而且 checkpoint 时间也会延长。
- 相关文档：[https://blog.csdn.net/weixin_41608066/article/details/109832538?spm=1001.2101.3001.6650.5&utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7EBlogCommendFromBaidu%7Edefault-5-109832538-blog-125300491.pc_relevant_default&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7EBlogCommendFromBaidu%7Edefault-5-109832538-blog-125300491.pc_relevant_default&utm_relevant_index=10]

开启文件合并功能后，文件将出现三种状态：..uncompacted、.uncompacted、compacted

| 阶段          | 描述                                                 |
| ------------- | ---------------------------------------------------- |
| ..uncompacted | 数据正在写入，还未准备好合并。                       |
| .uncompacted  | 数据已写入完毕，待合并，即等待checkpoint转换状态。   |
| compacted     | 在一个checkpoint间隔内的所有小文件，合并之后的文件。 |

**注意：**要和part文件的三种状态区分开，同一份文件可以既有inprogress标记也有uncompacted标记，即当前文件还没写完同时自然也未达到待合并状态。

<hr>

### 分区提交

分区提交的**前提**是建sink表时设置了分区字段，所以在创建sink表时，注意设置好 `partitioned by ()`，否则不出现分区。

分区提交包含两部分配置，触发器和触发策略：

- 触发器：根据哪个时间语义控制分区提交
- 触发策略：分区提交后的动作

#### 分区提交触发器

| 配置项                                    | 默认值       | 类型     | 描述                             |
| ----------------------------------------- | ------------ | -------- | -------------------------------- |
| sink.partition-commit.trigger             | process-time | String   | process-time<br />partition-time |
| sink.partition-commit.delay               | 0 s          | Duration | 在多长时间的等待后提交           |
| sink.partition-commit.watermark-time-zone | UTC          | String   | 是哪个时区上的timestamp          |

trigger有两种，一种基于^1^**处理时间**、一种基于^2^**分区时间**。

- **基于处理时间**：不需要抽取分区时间(**所以可以不按时间分区**)，也不需要生成水印，而是依赖于 ^1^**分区创建时间** 和 ^2^**当前系统时间**，当前系统时间超过 分区创建时间+delay 就提交分区。
  - 意味着 **Flink视角**，即不关注数据自身携带的时间属性，只关注Flink什么时候处理数据。以一条数据为例，该条数据进入Flink处理，Flink根据分区字段创建了它应分配到的分区，此时**分区创建时间**产生，然后等到**delay**时间到，就提交它所在的分区。
  - 这种提交方式，数据延迟或故障恢复(failover)会导致分区过早提交(因为这两种情况，都将导致**本该**在delay时间范围内到达的数据没到)。
  
- **基于分区时间**：依赖于分区值里抽取的时间和watermark，需要job有watermark生成、且分区需要是描述时间的值，例如每小时、每天。
- 意味着 **数据视角**，从数据自身的字段值里取出它所属的分区，当数据的watermark时间超过 分区时间值+delay 就提交分区。

#### 官方示例

| sink.partition-commit.trigger | sink.partition-commit.delay | 效果                                                         |
| ----------------------------- | --------------------------- | ------------------------------------------------------------ |
| process-time                  | 0s                          | 这种配置下，将无视分区创建时间，一旦分区内有数据就立刻提交，所以分区将可能多次提交。 |
| partition-time                | 1h                          | 假如分区是按小时分区，这种配置下的分区提交将**相对**最准确：因为当数据流出现该小时分区数据后，分区被创建出来，然后在继续等待的1小时内，按流数据的理想情况，该小时的数据都会在接下来的一个小时内出现。 |
| process-time                  | 1h                          | 尽可能提交准确，但是数据延迟或故障恢复(failover)会导致分区过早提交。 |

#### 延迟数据处理

> Late data processing: The record will be written into its partition when a record is supposed to be written into a partition that has already been committed, and then the committing of this partition will be triggered again.
>
> 延迟数据处理：这些数据将被写到各自对应的已提交过的分区里，然后再触发一次分区提交。

<hr>

#### 分区时间提取

| 配置项                                     | 默认值  | 类型   | 描述                                                         |
| ------------------------------------------ | ------- | ------ | ------------------------------------------------------------ |
| partition.time-extractor.kind              | default | String | 从分区值中提取时间的提取器，有**默认**和**自定义**两种；默认需要配置时间戳样式，自定义需要配置抽取类。 |
| partition.time-extractor.class             | (none)  | String | 实现PartitionTimeExtractor接口的抽取类。                     |
| partition.time-extractor.timestamp-pattern | (none)  | String | 默认构建方式下，使用者可以用分区字段去得到一个可用的时间戳样式(默认样式：yyyy-mm-dd hh:mm:ss)。 |

关于 partition.time-extractor.timestamp-pattern：

如果分区字段是dt，然后dt就是长这个样子：yyyy-mm-dd hh:mm:ss，就可以直接设置：'partition.time-extractor.timestamp-pattern'=`'$dt'`;

如果分区字段是dt、hour，就设置成：'partition.time-extractor.timestamp-pattern'=`'$dt $hour:00:00'`;

以此类推。

这个值的设置是 **为了和watermark配合使用**。

数据处理时，流数据将比较watermark与 该值+delay的时间，如果超过，就提交分区。



自定义时间提取(实现PartitionTimeExtractor接口)：

```java
public class HourPartTimeExtractor implements PartitionTimeExtractor {
    @Override
    public LocalDateTime extract(List<String> keys, List<String> values) {
        String dt = values.get(0);
        String hour = values.get(1);
		return Timestamp.valueOf(dt + " " + hour + ":00:00").toLocalDateTime();
	}
}
```



<hr>

#### 分区提交策略

分区提交策略定义了分区提交后要执行的动作。

- **metastore**：**刷新hive表元数据，只有hive表支持此策略**。
- success 文件：会在对应的分区文件夹下写一个空文件，文件名为_success。
- 自定义策略

| 配置项                                  | 默认值   | 类型   | 描述                                                         |
| --------------------------------------- | -------- | ------ | ------------------------------------------------------------ |
| sink.partition-commit.policy.kind       | (none)   | String | 提交策略旨在告知下游应用分区已写好，已可读。<br />metastore、_success文件、自定义。<br />支持同时配置多种策略。 |
| sink.partition-commit.policy.class      | (none)   | String | 自定义策略的类名。需实现PartitionCommitPolicy接口。          |
| sink.partition-commit.success-file.name | _SUCCESS | String | success文件的文件名，可通过这个配置项命名，默认'_success'。  |

自定义策略(实现PartitionCommitPolicy接口)：

```java
public class AnalysisCommitPolicy implements PartitionCommitPolicy {
    private HiveShell hiveShell;
	
    @Override
	public void commit(Context context) throws Exception {
	    if (hiveShell == null) {
	        hiveShell = createHiveShell(context.catalogName());
	    }
	    
        hiveShell.execute(String.format(
            "ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s = '%s') location '%s'",
	        context.tableName(),
	        context.partitionKeys().get(0),
	        context.partitionValues().get(0),
	        context.partitionPath()));
	    hiveShell.execute(String.format(
	        "ANALYZE TABLE %s PARTITION (%s = '%s') COMPUTE STATISTICS FOR COLUMNS",
	        context.tableName(),
	        context.partitionKeys().get(0),
	        context.partitionValues().get(0)));
	}
}
```

<hr>

## Sink并行度

向外部文件系统(包括Hive)写文件的并行度可通过表配置项修改(流数据、批数据都可以)，默认并行度是上游最后一个算子的并行度。

当配置不同于上游并行度的新并行度时，写文件的算子还有合并文件的算子将采用该新并行度。

<hr>

## 总结

主要是两方面内容：滚动策略、分区提交。

- 围绕滚动策略，涉及到sink表选择的格式：

​		行、列格式文件由不同的配置项决定文件的生成速度、合并文件功能是否起效。

- 围绕分区提交，涉及到不同的时间语义：

​		不同的时间语义，对落地的主观要求不同、配置项不同。


