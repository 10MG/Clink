# flink-jobs

## 介绍
flink-jobs为基于Flink的Java应用程序提供快速集成的能力，可通过继承FlinkJobsRunner快速构建基于Java的Flink流批一体应用程序。flink-jobs提供了数据源管理模块，通过flink-jobs运行Flink SQL会变得极其简单。而通过使用[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)，实现基于Java API启动flink-jobs应用程序，更可以将flink任务实现通过XML配置文件来管理。

## 起步

以基于SpringBoot的Maven项目为例

1.  pom.xml添加依赖（Flink等其他相关依赖此处省略），${flink-jobs.version}为版本号，可定义属性或直接使用版本号替换

```
<!-- https://mvnrepository.com/artifact/cn.tenmg/flink-jobs -->
<dependency>
    <groupId>cn.tenmg</groupId>
    <artifactId>flink-jobs</artifactId>
    <version>${flink-jobs.version}</version>
</dependency>
```

2.  配置文件application.properties

```
bootstrap.servers=192.168.100.181:9092,192.168.100.182:9092,192.168.100.183:9092
topics=topic1,topic2
auto.offset.reset=latest
group.id=flink-jobs
```

3.  编写配置类
```
@Configuration
@PropertySource(value = "application.properties")
public class Context {

	@Bean
	public Properties kafkaProperties(@Value("${bootstrap.servers}") String servers,
			@Value("${auto.offset.reset}") String autoOffsetReset, @Value("${group.id}") String groupId) {
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", servers);
		kafkaProperties.put("auto.offset.reset", autoOffsetReset);
		kafkaProperties.put("group.id", groupId);
		return kafkaProperties;
	}

}
```

4.  编写应用入口类

```
@ComponentScan("com.sinochem.flink.jobs")
public class App extends FlinkJobsRunner implements CommandLineRunner {

	@Autowired
	private ApplicationContext springContext;

	@Override
	protected StreamService getStreamService(String serviceName) {
		return (StreamService) springContext.getBean(serviceName);
	}

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}

}
```

5.  编写Flink流批一体服务

```
@Service
public class HelloWorldService implements StreamService {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6651233695630282701L;

	@Autowired
	private Properties kafkaProperties;

	@Value("${topics}")
	private String topics;

	@Override
	public void run(StreamExecutionEnvironment env, Arguments arguments) throws Exception {
		DataStream<String> stream;
		if (RuntimeExecutionMode.STREAMING.equals(arguments.getRuntimeMode())) {
			stream = env.addSource(new FlinkKafkaConsumer<String>(Arrays.asList(topics.split(",")),
					new SimpleStringSchema(), kafkaProperties));
		} else {
			stream = env.fromElements("Hello, World!");
		}
		stream.print();
	}

}
```

6.  到此，一个flink-jobs应用程序已完成，他可以通过各种方式运行。

- 在IDE环境中，可直接运行App类启动flink-jobs应用程序；

- 也可打包后，通过命令行提交给flink集群执行（通常在pom.xml配置org.apache.maven.plugins.shade.resource.ManifestResourceTransformer的mainClass为App这个类，请注意是完整类名）：`flink run /yourpath/yourfile.jar "{\"serviceName\":\"yourServiceName\"}"`，更多运行参数详见[运行参数](https://gitee.com/tenmg/flink-jobs#%E8%BF%90%E8%A1%8C%E5%8F%82%E6%95%B0arguments)。

- 此外，通过使用[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)可以通过Java API的方式启动flink-jobs应用程序，这样启动操作就可以轻松集成到其他系统中（例如Java Web程序）。

## 快速入门

详见https://gitee.com/tenmg/flink-jobs-quickstart

## 运行参数（arguments）
flink-jobs应用程序的运行参数通过JSON格式的字符串（注意，如果是命令行运行，JSON格式字符串前后需加上双引号或单引号，JSON格式字符串内部的双引号或单引号则需要转义）或者一个.json文件提供，结构如下：

```
{
    "serviceName": "specifyName",
    "runtimeMode": "BATCH"/"STREAMING"/"AUTOMATIC",
    "params": {
    	"key1": "value1",
    	"key2": "value2",
        …
    },
    "operates": [{
        "script": "specifySQL",
        "type": "ExecuteSql"
    }, {
        "dataSource": "kafka",
        "script": "specifySQL",
        "type": "ExecuteSql"
    }, {
        "saveAs": "specifyTemporaryTableName",
        "catalog": "specifyCatalog",
        "script": "specifySQL",
        "type": "SqlQuery"
   }, … ]
}
```

属性        | 类型                | 必需 | 说明
------------|--------------------|----|--------
serviceName | `String`             | 否 | 运行的服务名称。该名称由用户定义并实现根据服务名称获取服务的方法，flink-jobs则在运行时调用并确定运行的实际服务。在运行SQL任务时，通常指定operates，而无需指定serviceName。
runtimeMode | `String`             | 否 | 运行模式。可选值："BATCH"/"STREAMING"/"AUTOMATIC"，相关含义详见[Flink](https://flink.apache.org)官方文档。
params      | `Map<String,Object>` | 否 | 参数查找表。通常可用于SQL中，也可以在自定义服务中通过arguments参数获取。
operates    | `List<Operate>`      | 否 | 操作列表。目前支持类型为[Bsh](#bsh%E6%93%8D%E4%BD%9C)、[ExecuteSql](#executesql%E6%93%8D%E4%BD%9C)、[SqlQuery](#sqlquery%E6%93%8D%E4%BD%9C)，[Jdbc](#jdbc%E6%93%8D%E4%BD%9C)和[DataSync](https://gitee.com/tenmg/flink-jobs#datasync%E6%93%8D%E4%BD%9C)5种类型操作。

### Bsh操作

Bsh操作的作用是运行基于Beanshell的java代码，支持版本：1.1.0+，相关属性及说明如下：

属性   | 类型        | 必需 | 说明
-------|-------------|----|--------
type   | `String`    | 是 | 操作类型。这里是"Bsh"。
saveAs | `String`    | 否 | 操作结果另存为一个新的变量的名称。变量的值是基于Beanshell的java代码的返回值（通过`return xxx;`表示）。
vars   | `List<Var>` | 否 | 参数声明列表。
java   | `String`    | 是 | java代码。注意：使用泛型时，不能使用尖括号声明泛型。例如，使用Map不能使用“Map<String , String> map = new HashMap<String , String>();”，但可以使用“Map map = new HashMap();”。

#### Var

属性   | 类型    | 必需 | 说明
------|----------|----|--------
name  | `String` | 是 | Beanshell中使用的变量名称
value | `String` | 否 | 变量对应的值的名称。默认与name相同。flink-jobs会从参数查找表中查找名称为value值的参数值，如果指定参数存在且不是null，则该值作为该参数的值；否则，使用value值作为该变量的值。

### ExecuteSql操作

ExecuteSql操作的作用是运行基于[DSL](https://gitee.com/tenmg/dsl)的SQL代码，支持版本：1.1.0+，相关属性及说明如下：

属性       | 类型     | 必需 | 说明
-----------|----------|----|--------
type       | `String` | 是 | 操作类型。这里是"ExecuteSql"。
saveAs     | `String` | 否 | 操作结果另存为一个新的变量的名称。变量的值是flink的`tableEnv.executeSql(statement);`的返回值。
dataSource | `String` | 否 | 使用的数据源名称。
catalog    | `String` | 否 | 执行SQL使用的Flink SQL的catalog名称。
script     | `String` | 是 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。由于Flink SQL不支持DELETE、UPDATE语句，因此如果配置的SQL脚本是DELETE或者UPDATE语句，该语句将在程序main函数中采用JDBC执行。

### SqlQuery操作

SqlQuery操作的作用是运行基于[DSL](https://gitee.com/tenmg/dsl)的SQL查询代码，支持版本：1.1.0+，相关属性及说明如下：

属性       | 类型  | 必需 | 说明
-----------|--------|----|--------
saveAs     | `String` | 否 | 查询结果另存为临时表的表名及操作结果另存为一个新的变量的名称。变量的值是flink的`tableEnv.executeSql(statement);`的返回值。
catalog    | `String` | 否 | 执行SQL使用的Flink SQL的catalog名称。
script     | `String` | 是 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。

### Jdbc操作

Jdbc操作的作用是运行基于[DSL](https://gitee.com/tenmg/dsl)的JDBC SQL代码，支持版本：1.1.1+，相关属性及说明如下：

属性       | 类型     | 必需 | 说明
-----------|----------|----|--------
type       | `String` | 是 | 操作类型。这里是"Jdbc"。
saveAs     | `String` | 否 | 执行结果另存为一个新的变量的名称。变量的值是执行JDBC指定方法的返回值。
dataSource | `String` | 是 | 使用的数据源名称。
method     | `String` | 否 | 调用的JDBC方法。默认是"executeLargeUpdate"。
script     | `String` | 是 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。

目标JDBC SQL代码是在flink-jobs应用程序的main函数中运行的。

### DataSync操作

DataSync操作的作用是运行基于Flink SQL的流式任务实现数据同步，其原理是根据配置信息自动生成并执行Flink SQL。支持版本：1.1.2+，相关属性及说明如下：

属性       | 类型            | 必需 | 说明
-----------|----------------|----|--------
type       | `String`       | 是 | 操作类型。这里是"DataSync"。
saveAs     | `String`       | 否 | 执行结果另存为一个新的变量的名称。变量的值是执行`INSERT`语句返回的`org.apache.flink.table.api.TableResult`对象。
from       | `String`       | 是 | 来源数据源名称。目前仅支持Kafka数据源。
topic      | `String`       | 否 | Kafka主题。也可在fromConfig中配置`topic=xxx`。
fromConfig | `String`       | 否 | 来源配置。例如：`properties.group.id=flink-jobs`。
to         | `String`       | 是 | 目标数据源名称，目前仅支持JDBC数据源。
toConfig   | `String`       | 是 | 目标配置。例如：`sink.buffer-flush.max-rows = 0`。
table      | `String`       | 是 | 同步数据表名。
columns    | `List<Column>` | 否 | 同步数据列。当开启智能模式时，会自动获取列信息。
primaryKey | `String`       | 否 | 主键，多个列名以“,”分隔。当开启智能模式时，会自动获取主键信息。
smart      | `Boolean`      | 否 | 是否开启智能模式。不设置时，根据全局配置确定是否开启智能模式，全局默认配置为`data.sync.smart=true`。

#### Column

属性     | 类型     | 必需 | 说明
---------|----------|----|--------
fromName | `String` | 是 | 来源列名。
fromType | `String` | 否 | 来源数据类型。如果缺省，则如果开启智能模式会自动获取目标数据类型作为来源数据类型，如果关闭智能模式则必填。
toName   | `String` | 否 | 目标列名。默认为来源列名。
toType   | `String` | 否 | 目标列数据类型。如果缺省，则如果开启智能模式会自动获取，如果关闭智能模式则默认为来源列数据类型。
script   | `String` | 否 | 自定义脚本。通常是需要进行函数转换时使用。

#### 相关配置

[配置文件](https://gitee.com/tenmg/flink-jobs#%E9%85%8D%E7%BD%AE%E6%96%87%E4%BB%B6)中可以增加数据同步的相关配置，各配置说明如下：

##### data.sync.smart

是否开启数据同步的智能模式，默认为`true`。开启智能模式的潜台词是指，自动通过已实现的元数据获取器（也可自行扩展）获取同步的目标库的元数据以生成Flink SQL的源表（Source Table）、目标表（Slink Table）和相应的插入语句（`INSERT INTO … SELECT … FROM …`）。

##### data.sync.from_table_prefix

源表（Source Table）表名的前缀，默认为`SOURCE_`。该前缀和目标表（Slink Table）的表名拼接起来即为源表的表名。

##### data.sync.group_id_prefix

数据同步时消费消息队列（Kafka）的`groupid`的前缀，默认为`flink-jobs-data-sync.`。该前缀和目标表（Slink Table）的表名拼接起来构成消费消息队列（Kafka）的`groupid`，但用户在任务中指定`properties.group.id`的除外。

##### data.sync.metadata.getter.*

用户可以需要实现`cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter`接口并通过该配置项来扩展元数据获取器，也可以使用自实现的元数据获取器来替换原有的元数据获取器。默认配置为：

```
data.sync.metadata.getter.jdbc=cn.tenmg.flink.jobs.operator.data.sync.getter.JDBCMetaDataGetter
data.sync.metadata.getter.starrocks=cn.tenmg.flink.jobs.operator.data.sync.getter.StarrocksMetaDataGetter
```

##### data.sync.columns.convert

1.1.3版本开始支持`data.sync.columns.convert`，用于配置数据同步的SELECT子句的列转换函数，可使用`#columnName`占位符表示当前列名，flink-jobs会在运行时将转换函数作为一个SQL片段一个`INSERT INTO …… SELECT …… FROM ……`语句的的一个片段。

示例1：

```

data.sync.columns.convert=BIGINT,TIMESTAMP:TO_TIMESTAMP(FROM_UNIXTIME(#columnName/1000 - 8*60*60, 'yyyy-MM-dd HH:mm:ss'))

```

上述配置旨在将`BIGINT`类型表示的时间转换为`TIMESTAMP`类型的时间，同时减去8个小时（时区转换，Debezium的时间通常是UTC时间）转换为北京时间。该配置包含几层含义：

1. 如果没有指明同步的列信息，且开启智能模式（配置`data.sync.smart=true`），则从目标库中加载元数据，确定列名并自动将JDBC类型对应到Flink SQL的类型上，并作为创建目标表（`Sink`表）的依据。当某字段类型为`TIMESTAMP`时，会在同步时应用该转换函数。此时，其源表对应字段类型则为`BIGINT`，否则源表对应字段类型和目标表（`Sink`表）的一致；字段名方面，默认源表对应字段名和目标表（`Sink`表）字段名一致。最后根据列的相关信息生成并执行相关同步SQL。

2. 如果部分指定了同步的列信息，且开启智能模式（配置`data.sync.smart=true`），则从目标库中加载元数据，并自动补全用户未配置的部分列信息后，再生成并执行相关同步SQL。

3. 如果完全指明同步的列信息，则根据指定的信息分别生成并执行相关同步SQL。

示例2：

```
data.sync.columns.convert=BIGINT,TIMESTAMP:TO_TIMESTAMP(FROM_UNIXTIME(#columnName/1000 - 8*60*60, 'yyyy-MM-dd HH:mm:ss'));INT,DATE:TO_TIMESTAMP(FROM_UNIXTIME(#columnName/1000 - 8*60*60, 'yyyy-MM-dd HH:mm:ss'))
```

##### data.sync.timestamp.case_sensitive

1.1.4版本开始支持`data.sync.timestamp.case_sensitive`，用于配置数据同步的时间戳列名的大小写敏感性，他是flink-jobs在识别时间戳列时的策略配置。由于Flink SQL通常是大小写敏感的，因此该值默认为`true`，用户可以根据需要在配置文件中调整配置。大小写敏感的情况下，有关时间戳的列名必须按照实际建表的列名完全匹配，否则无法识别；大小写不敏感，则在匹配时间戳字段时对字段名忽略大小写。

##### data.sync.timestamp.from_type

1.1.4版本开始支持`data.sync.timestamp.from_type`，用于配置数据同步的来源时间戳列的默认类型，默认值为`TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL`，这是Flink SQL所支持的几种变更数据捕获（CDC）工具（Debezium/Canal/Maxwell）都支持的。

##### data.sync.timestamp.to_type

1.1.4版本开始支持`data.sync.timestamp.to_type`，用于配置数据同步的目标时间戳列的默认类型，默认值为`TIMESTAMP(3)`，与`data.sync.timestamp.from_type`的默认值具有对应关系。

##### data.sync.*.from_type

1.1.4版本开始支持`data.sync.*.from_type`，用于配置数据同步增加的特定时间戳列的来源类型，如果没有配置则使用`data.sync.timestamp.from_type`的值。典型的值为`TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL`或`TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL`（目前仅Debezium支持），可根据具体情况确定。

##### data.sync.*.to_type

1.1.4版本开始支持`data.sync.*.to_type`，用于配置数据同步增加的特定时间戳列的目标类型，如果没有配置则使用`data.sync.timestamp.to_type`的值。典型的值为`TIMESTAMP(3)`，具体精度可根据数据源的精度确定。

##### data.sync.*.strategy

1.1.4版本开始支持`data.sync.*.strategy`，用于配置数据同步特定时间戳列的同步策略，可选值：`both/from/to`，both表示来源列和目标列均创建，from表示仅创建原来列，to表示仅创建目标列, 默认为both。

##### data.sync.*.script

1.1.4版本开始支持`data.sync.*.script`，用于配置数据同步特定时间戳列的自定义脚本（`SELECT`子句的片段），通常是一个函数或等效表达，例如`NOW()`或`CURRENT_TIMESTAMP`。结合`data.sync.*.strategy=to`使用，可实现写入处理时间的效果。

##### 配置示例

以下是一个使用Debezium实现数据同步的典型数据同步配置示例，不仅完成了时间格式和时区的转换，还完成了时间戳的自动写入（智能模式下，时间戳是否写入取决于目标表中对应列是否存在）：

```
#数据同步类型转换配置
data.sync.columns.convert=BIGINT,TIMESTAMP:TO_TIMESTAMP(FROM_UNIXTIME(#columnName/1000 - 8*60*60, 'yyyy-MM-dd HH:mm:ss'));INT,DATE:TO_TIMESTAMP(FROM_UNIXTIME(#columnName/1000 - 8*60*60, 'yyyy-MM-dd HH:mm:ss'))
#数据同步自动添加时间戳字段
data.sync.timestamp.columns=INGESTION_TIMESTAMP,EVENT_TIMESTAMP,ETL_TIMESTAMP
#数据同步自动添加EVENT_TIMESTAMP时间戳字段的类型配置
data.sync.EVENT_TIMESTAMP.from_type=TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL
data.sync.EVENT_TIMESTAMP.to_type=TIMESTAMP(3)
data.sync.EVENT_TIMESTAMP.to_type=TIMESTAMP(3)
#ETL_TIMESTAMP列取当前时间戳，策略设置为to，仅创建目标列而不创建来源列
data.sync.ETL_TIMESTAMP.strategy=to
data.sync.ETL_TIMESTAMP.script=NOW()
#INGESTION_TIMESTAMP字段类型使用默认配置，这里无需指定
```

## 配置文件

默认的配置文件为flink-jobs.properties（注意：需在classpath下），可通过flink-jobs-context-loader.properties配置文件的`config.location`修改配置文件路径和名称。配置项的值允许通过占位符`${}`引用，例如`key=${anotherKey}`。

### 数据源配置

每个数据源有一个唯一的命名，数据源配置以“datasource”为前缀，以“.”作为分隔符，格式为`datasource.${name}.${key}=${value}`。其中，第一和第二个“.”符号之间的是数据源名称，第二个“.”符号之后和“=”之前的是该数据源具体的配置项，“=”之后的是该配置项的值。数据源的配置项与[Flink](https://flink.apache.org)保持一致，具体配置项详见[Flink官方文档](https://flink.apache.org)。以下给出部分常用数据源配置示例：

```
#FlinkSQL数据源配置
#Debezium
#配置名称为kafka的数据源
datasource.kafka.connector=kafka
datasource.kafka.properties.bootstrap.servers=192.168.1.101:9092,192.168.1.102:9092,192.168.1.103:9092
datasource.kafka.properties.group.id=flink-jobs
datasource.kafka.scan.startup.mode=earliest-offset
datasource.kafka.format=debezium-json
datasource.kafka.debezium-json.schema-include=true

#PostgreSQL
#配置名称为bidb的数据源
datasource.bidb.connector=jdbc
datasource.bidb.driver=org.postgresql.Driver
datasource.bidb.url=jdbc:postgresql://192.168.1.104:5432/bidb
datasource.bidb.username=your_name
datasource.bidb.password=your_password

#引用配置文件内的另一个配置
#配置名称为syndb的数据源
datasource.syndb.connector=${datasource.bidb.connector}
datasource.syndb.driver=${datasource.bidb.driver}
datasource.syndb.url=${datasource.bidb.url}?currentSchema=syndb
datasource.syndb.username=${datasource.bidb.username}
datasource.syndb.password=${datasource.bidb.password}

#MySQL
#配置名称为kaorder的数据源
datasource.kaorder.connector=jdbc
datasource.kaorder.driver=com.mysql.cj.jdbc.Driver
datasource.kaorder.url=jdbc:mysql://192.168.1.105:3306/kaorder?useSSL=false&serverTimezone=Asia/Shanghai
datasource.kaorder.username=your_name
datasource.kaorder.password=your_password

#SQLServer
#配置名称为sqltool的数据源
datasource.sqltool.connector=jdbc
datasource.sqltool.driver=org.postgresql.Driver
datasource.sqltool.url=jdbc:sqlserver://192.168.1.106:1433;DatabaseName=sqltool;
datasource.sqltool.username=your_name
datasource.sqltool.password=your_password

#Hive
#配置名称为hivedb的数据源
datasource.hivedb.type=hive
datasource.hivedb.default-database=default
datasource.hivedb.hive-conf-dir=/etc/hive/conf

#StarRocks
#配置名称为starrocks的数据源
datasource.starrocks.jdbc-url=jdbc:mysql://192.168.10.140:9030
datasource.starrocks.load-url=192.168.10.140:8030
datasource.starrocks.username=your_name
datasource.starrocks.password=your_password
datasource.starrocks.sink.properties.column_separator=\\x01
datasource.starrocks.sink.properties.row_delimiter=\\x02
# the flushing time interval, range: [1000ms, 3600000ms].
datasource.starrocks.sink.buffer-flush.interval-ms=10000
# max retry times of the stream load request, range: [0, 10].
datasource.starrocks.sink.max-retries=3
datasource.starrocks.connector=starrocks
datasource.starrocks.database-name=your_db
```

### Table API & SQL

[Flink](http://)的Table API & SQL配置除了在Flink配置文件中指定之外，也可以在flink-jobs的配置文件中指定。例如：

`table.exec.sink.not-null-enforcer=drop`

注意：如果是在flink-jobs的配置文件中配置这些参数，当执行自定义Java服务时，只有通过`FlinkJobsContext.getOrCreateStreamTableEnvironment()`或`FlinkJobsContext.getOrCreateStreamTableEnvironment(env)`方法获取的`StreamTableEnvironment`执行Table API & SQL，这些配置才会生效。

## 系统集成

[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)实现了使用XML配置文件来管理flink-jobs任务，这样开发Flink SQL任务会显得非常简单；同时，用户自定义的flink-jobs服务也可以被更轻松得集成到其他系统中。XML文件具有良好的可读性，并且在IDE环境下能够对配置进行自动提示。具体使用方法详见[flink-jobs-launcher开发文档](https://gitee.com/tenmg/flink-jobs-launcher)，以下介绍几种通过XML管理的flink-jobs任务：

### 运行自定义服务

以下为一个自定义服务任务XML配置文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<flink-jobs xmlns="http://www.10mg.cn/schema/flink-jobs"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/flink-jobs http://www.10mg.cn/schema/flink-jobs.xsd"
	jar="/yourPath/yourJar.jar" serviceName="yourServiceName">
</flink-jobs>
```

### 运行批处理SQL

以下为一个简单订单量统计SQL批处理任务XML配置文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<flink-jobs xmlns="http://www.10mg.cn/schema/flink-jobs"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/flink-jobs http://www.10mg.cn/schema/flink-jobs.xsd"
	jar="/yourPath/yourJar.jar">
	<!--任务运行参数 -->
	<params>
		<param name="beginDate">2021-01-01</param>
		<param name="endDate">2021-07-01</param>
	</params>

	<!-- 使用名为hivedb的数据源创建名为hive的catalog -->
	<execute-sql dataSource="hivedb">
		<![CDATA[
			create catalog hive
		]]>
	</execute-sql>
	<!--加载hive模块 -->
	<execute-sql>
		<![CDATA[
			load module hive
		]]>
	</execute-sql>
	<!--使用hive,core模块 -->
	<execute-sql>
		<![CDATA[
			use modules hive,core
		]]>
	</execute-sql>
	<!-- 使用名为pgdb的数据源创建表order_stats_daily（如果源表名和建表语句指定的表名不一致，可以通过 WITH ('table-name' 
		= 'actrual_table_name') 来指定） -->
	<execute-sql dataSource="pgdb">
		<![CDATA[
			CREATE TABLE order_stats_daily (
			  stats_date DATE,
			  `count` BIGINT,
			  PRIMARY KEY (stats_date) NOT ENFORCED
			) WITH ('sink.buffer-flush.max-rows' = '0')
		]]>
	</execute-sql>
	<!-- 使用hive catalog查询，并将结果存为临时表tmp，tmp放在默认的default_catalog中 -->
	<sql-query saveAs="tmp" catalog="hive">
		<![CDATA[
			select cast(to_date(o.business_date) as date) stats_date, count(*) `count` from odc_order_info_par o where o.business_date >= :beginDate and o.business_date < :endDate group by cast(to_date(o.business_date) as date)
		]]>
	</sql-query>
	<!-- 删除原有数据order_stats_daily（FLINK SQL不支持DELETE，此处执行的是JDBC）-->
	<execute-sql dataSource="pgdb">
		<![CDATA[
			delete from order_stats_daily where stats_date >= :beginDate and stats_date < :endDate
		]]>
	</execute-sql>
	<!-- 数据插入。实际上Flink最终将执行Upsert语法 -->
	<execute-sql>
		<![CDATA[
			INSERT INTO order_stats_daily(stats_date,`count`) SELECT stats_date, `count` FROM tmp
		]]>
	</execute-sql>
</flink-jobs>
```

### 运行流处理SQL

以下为通过Debezium实现异构数据库同步任务XML配置文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<flink-jobs xmlns="http://www.10mg.cn/schema/flink-jobs"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/flink-jobs http://www.10mg.cn/schema/flink-jobs.xsd">
	<!-- Flink内创建SOURCE数据库 -->
	<!-- <execute-sql>
		<![CDATA[
		CREATE DATABASE SOURCE
		]]>
	</execute-sql> -->
	<!-- 使用SOURCE数据库执行Flink SQL -->
	<!-- <execute-sql>
		<![CDATA[
		USE SOURCE
		]]>
	</execute-sql> -->
	<!-- 上述两步操作是非必须的，只是为了Flink自动生成的作业名称更容易识别 -->
	<!-- 定义名为kafka的数据源的订单明细表 -->
	<execute-sql dataSource="kafka">
		<![CDATA[
		CREATE TABLE KAFKA_ORDER_DETAIL (
		  DETAIL_ID STRING,
		  ORDER_ID STRING,
		  ITEM_ID STRING,
		  ITEM_CODE STRING,
		  ITEM_NAME STRING,
		  ITEM_TYPE STRING,
		  ITEM_SPEC STRING,
		  ITEM_UNIT STRING,
		  ITEM_PRICE DECIMAL(12, 2),
		  ITEM_QUANTITY DECIMAL(12, 2),
		  SALE_PRICE DECIMAL(12, 2),
		  SALE_AMOUNT DECIMAL(12, 2),
		  SALE_DISCOUNT DECIMAL(12, 2),
		  SALE_MODE STRING,
		  CURRENCY STRING,
		  SUPPLY_TYPE STRING,
		  SUPPLY_CODE STRING,
		  REMARKS STRING,
		  CREATE_BY STRING,
		  CREATE_TIME BIGINT,
		  UPDATE_BY STRING,
		  UPDATE_TIME BIGINT,
		  OIL_GUN STRING,
		  EVENT_TIME TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
		  PRIMARY KEY (DETAIL_ID) NOT ENFORCED
		) WITH ('topic' = 'kaorder1.kaorder.order_detail', 'properties.group.id' = 'flink-jobs_source_order_detail')
		]]>
	</execute-sql>
	<!-- 定义名为source的数据源的订单明细表 -->
	<execute-sql dataSource="source">
		<![CDATA[
		CREATE TABLE ORDER_DETAIL (
		  DETAIL_ID STRING,
		  ORDER_ID STRING,
		  ITEM_ID STRING,
		  ITEM_CODE STRING,
		  ITEM_NAME STRING,
		  ITEM_TYPE STRING,
		  ITEM_SPEC STRING,
		  ITEM_UNIT STRING,
		  ITEM_PRICE DECIMAL(12, 2),
		  ITEM_QUANTITY DECIMAL(12, 2),
		  SALE_PRICE DECIMAL(12, 2),
		  SALE_AMOUNT DECIMAL(12, 2),
		  SALE_DISCOUNT DECIMAL(12, 2),
		  SALE_MODE STRING,
		  CURRENCY STRING,
		  SUPPLY_TYPE STRING,
		  SUPPLY_CODE STRING,
		  REMARKS STRING,
		  CREATE_BY STRING,
		  CREATE_TIME TIMESTAMP(3),
		  UPDATE_BY STRING,
		  UPDATE_TIME TIMESTAMP(3),
		  OIL_GUN STRING,
		  EVENT_TIME TIMESTAMP(3),
		  PRIMARY KEY (DETAIL_ID) NOT ENFORCED
		)
		]]>
	</execute-sql>
	<!-- 将kafka订单明细数据插入到source数据源的订单明细表中 -->
	<execute-sql>
		<![CDATA[
		INSERT INTO ORDER_DETAIL(
		  DETAIL_ID,
		  ORDER_ID,
		  ITEM_ID,
		  ITEM_CODE,
		  ITEM_NAME,
		  ITEM_TYPE,
		  ITEM_SPEC,
		  ITEM_UNIT,
		  ITEM_PRICE,
		  ITEM_QUANTITY,
		  SALE_PRICE,
		  SALE_AMOUNT,
		  SALE_DISCOUNT,
		  SALE_MODE,
		  CURRENCY,
		  SUPPLY_TYPE,
		  SUPPLY_CODE,
		  REMARKS,
		  CREATE_BY,
		  CREATE_TIME,
		  UPDATE_BY,
		  UPDATE_TIME,
		  OIL_GUN,
		  EVENT_TIME
		)
		SELECT
		  DETAIL_ID,
		  ORDER_ID,
		  ITEM_ID,
		  ITEM_CODE,
		  ITEM_NAME,
		  ITEM_TYPE,
		  ITEM_SPEC,
		  ITEM_UNIT,
		  ITEM_PRICE,
		  ITEM_QUANTITY,
		  SALE_PRICE,
		  SALE_AMOUNT,
		  SALE_DISCOUNT,
		  SALE_MODE,
		  CURRENCY,
		  SUPPLY_TYPE,
		  SUPPLY_CODE,
		  REMARKS,
		  CREATE_BY,
		  TO_TIMESTAMP(FROM_UNIXTIME(CREATE_TIME/1000, 'yyyy-MM-dd HH:mm:ss')) CREATE_TIME,
		  UPDATE_BY,
		  TO_TIMESTAMP(FROM_UNIXTIME(CREATE_TIME/1000, 'yyyy-MM-dd HH:mm:ss')) UPDATE_TIME,
		  OIL_GUN,
		  EVENT_TIME
		FROM KAFKA_ORDER_DETAIL
		]]>
	</execute-sql>
</flink-jobs>
```

### 运行数据同步任务

以下为通过kafka（配合Debezium、Cannal或Maxwell等）实现异构数据库同步任务XML配置文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<flink-jobs xmlns="http://www.10mg.cn/schema/flink-jobs"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/flink-jobs http://www.10mg.cn/schema/flink-jobs-1.1.2.xsd">
	<data-sync table="od_order_info" to="data_skyline"
		from="kafka" topic="testdb.testdb.od_order_info">
		<!-- 在数据源和目标库表结构相同（字段名及类型均相同）的情况下，智能模式可自动从目标库获取表元数据信息，只要少量配就能完成数据同步。 -->
		<!-- 在数据源和目标库表结构不同（字段名或类型不同）的情况，需要自定义列的差异信息，例如自定来源类型和转换函数： -->
		<column fromName="UPDATE_TIME" fromType="BIGINT">TO_TIMESTAMP(FROM_UNIXTIME(UPDATE_TIME/1000, 'yyyy-MM-dd HH:mm:ss'))</column>
		<!-- 另外，如果关闭智能模式，需要列出所有列的信息详细信息。 -->
	</data-sync>
</flink-jobs>
```

## DSL

[DSL](https://gitee.com/tenmg/dsl)的全称是动态脚本语言(Dynamic Script Language)，它使用特殊字符`#[]`标记脚本片段，片段内使用若干个参数，一起构成动态片段（支持嵌套使用）。当使用flink-jobs运行Flink SQL时，判断实际传入参数值是否为空（`null`）决定是否保留该片段（同时自动去除`#[]`），形成最终可执行的脚本提交执行。使用[DSL](https://gitee.com/tenmg/dsl)可以有效避免程序员手动拼接繁杂的SQL，使得程序员能从繁杂的业务逻辑中解脱出来。

### 简单例子

假设有如下动态查询语句：

```
SELECT
  *
FROM STAFF_INFO S
WHERE S.STATUS = 'VALID'
#[AND S.STAFF_ID = :staffId]
#[AND S.STAFF_NAME LIKE :staffName]
```

参数staffId为空（`null`），而staffName为非空（非`null`）时，实际执行的语句为：

```
SELECT
   *
 FROM STAFF_INFO S
 WHERE S.STATUS = 'VALID'
 AND S.STAFF_NAME LIKE :staffName
```

相反，参数staffName为空（`null`），而staffId为非空（非`null`）时，实际执行的语句为：


```
SELECT
   *
 FROM STAFF_INFO S
 WHERE S.STATUS = 'VALID'
 AND S.STAFF_ID = :staffId
```

或者，参数staffId、staffName均为空（`null`）时，实际执行的语句为：

```
SELECT
   *
 FROM STAFF_INFO S
 WHERE S.STATUS = 'VALID'
```

最后，参数staffId、staffName均为非空（非`null`）时，实际执行的语句为：

```
SELECT
   *
 FROM STAFF_INFO S
 WHERE S.STATUS = 'VALID'
 AND S.STAFF_ID = :staffId
 AND S.STAFF_NAME LIKE :staffName
```

通过上面这个小例子，我们看到了动态脚本语言（DSL）的魔力。这种魔力的来源是巧妙的运用了一个值：空(`null`)，因为该值往往在SQL中很少用到，而且即便使用也是往往作为特殊的常量使用，比如：
```
NVL(EMAIL,'无')
```
和
```
WHERE EMAIL IS NOT NULL
```
等等。

### 参数

#### 普通参数

使用`:`加参数名表示普通参数，例如，:staffName。

#### 嵌入式参数

使用`#`加参数名表示（例如，#staffName）嵌入式参数，嵌入式参数会被以字符串的形式嵌入到脚本中。 **值得注意的是：在SQL脚本中使用嵌入式参数，会有SQL注入风险，一定注意不要使用前端传参直接作为嵌入式参数使用** 。1.1.3版本开始支持嵌入式参数，即对应dsl版本为1.2.2，单独升级dsl也可以支持。

#### 动态参数

动态参数是指，根据具体情况确定是否在动态脚本中生效的参数，动态参数是动态片段的组成部分。动态参数既可以是普通参数，也可以嵌入式参数。

#### 静态参数

静态参数是相对动态参数而言的，它永远会在动态脚本中生效。在动态片段之外使用的参数就是静态参数。静态参数既可以是普通参数，也可以嵌入式参数。

#### 参数访问符

参数访问符包括两种，即`.`和`[]`, 使用`Map`传参时，优先获取键相等的值，只有键不存在时才会将键降级拆分一一访问对象，直到找到参数并返回，或未找到返回`null`。其中`.`用来访问对象的属性，例如`:staff.name`、`#staff.age`；`[]`用来访问数组、集合的元素，例如`:array[0]`、`#map[key]`。理论上，支持任意级嵌套使用，例如`:list[0][1].name`、`#map[key][1].staff.name`。1.1.3版本开始支持参数访问符，即对应dsl版本为1.2.2，单独升级dsl也可以支持。

### 进一步了解

[DSL](https://gitee.com/tenmg/dsl)中的动态片段可以是任意使用特殊字符`#[]`标记且包含参数的片段，它可以应用于各种SQL语句中，包括但不限于`CREATE`、`DROP`、`SELECT`、`INSERT`、`UPDATE`、`DELETE`。更多有关[DSL](https://gitee.com/tenmg/dsl)的介绍，详见[https://gitee.com/tenmg/dsl](https://gitee.com/tenmg/dsl)

## 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request

## 相关链接

flink-jobs-launcher开源地址：https://gitee.com/tenmg/flink-jobs-launcher

DSL开源地址：https://gitee.com/tenmg/dsl

Flink官网：https://flink.apache.org

Debezuim官网：https://debezium.io