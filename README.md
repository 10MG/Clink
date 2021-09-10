# flink-jobs

### 介绍
flink-jobs为基于Flink的Java应用程序提供快速集成的能力，可通过继承FlinkJobsRunner快速构建基于Java的Flink流批一体应用程序。还可以通过使用[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)，实现基于Java API启动flink-jobs应用程序。

### 起步

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
bootstrap.servers=192.168.10.40:9092,192.168.10.78:9092,192.168.10.153:9092
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

### 快速入门

详见https://gitee.com/tenmg/flink-jobs-quickstart

### 运行参数（arguments）
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
operates    | `List<Operate>`      | 否 | 操作列表。目前支持类型为[Bsh](#bsh%E6%93%8D%E4%BD%9C)、[ExecuteSql](#executesql%E6%93%8D%E4%BD%9C)、[SqlQuery](#sqlquery%E6%93%8D%E4%BD%9C)和[Jdbc](#jdbc%E6%93%8D%E4%BD%9C)四种操作。

#### Bsh操作

Bsh操作的作用是运行基于Beanshell的java代码，相关属性及说明如下：

属性   | 类型        | 必需 | 说明
-------|-------------|----|--------
type   | `String`    | 是 | 操作类型。这里是"Bsh"。
saveAs | `String`    | 否 | 操作结果另存为一个新的变量的名称。变量的值是基于Beanshell的java代码的返回值（通过`return xxx;`表示）。
vars   | `List<Var>` | 否 | 参数声明列表。
java   | `String`    | 是 | java代码。注意：使用泛型时，不能使用尖括号声明泛型。例如，使用Map不能使用“Map<String , String> map = new HashMap<String , String>();”，但可以使用“Map map = new HashMap();”。

Var相关属性及说明如下：

属性   | 类型    | 必需 | 说明
------|----------|----|--------
name  | `String` | 是 | Beanshell中使用的变量名称
value | `String` | 否 | 变量对应的值的名称。默认与name相同。flink-jobs会从参数查找表中查找名称为value值的参数值，如果指定参数存在且不是null，则该值作为该参数的值；否则，使用value值作为该变量的值。

#### ExecuteSql操作

ExecuteSql操作的作用是运行基于[DSL](https://gitee.com/tenmg/dsl)的SQL代码，相关属性及说明如下：

属性       | 类型     | 必需 | 说明
-----------|----------|----|--------
type       | `String` | 是 | 操作类型。这里是"ExecuteSql"。
saveAs     | `String` | 否 | 操作结果另存为一个新的变量的名称。变量的值是flink的`tableEnv.executeSql(statement);`的返回值。
dataSource | `String` | 否 | 使用的数据源名称。
catalog    | `String` | 否 | 执行SQL使用的Flink SQL的catalog名称。
script     | `String` | 是 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。由于Flink SQL不支持DELETE、UPDATE语句，因此如果配置的SQL脚本是DELETE或者UPDATE语句，该语句将在程序main函数中采用JDBC执行。

#### SqlQuery操作

SqlQuery操作的作用是运行基于[DSL](https://gitee.com/tenmg/dsl)的SQL查询代码，相关属性及说明如下：

属性       | 类型  | 必需 | 说明
-----------|--------|----|--------
saveAs     | `String` | 否 | 查询结果另存为临时表的表名及操作结果另存为一个新的变量的名称。变量的值是flink的`tableEnv.executeSql(statement);`的返回值。
catalog    | `String` | 否 | 执行SQL使用的Flink SQL的catalog名称。
script     | `String` | 是 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。

#### Jdbc操作

Jdbc操作的作用是运行基于[DSL](https://gitee.com/tenmg/dsl)的JDBC SQL代码，相关属性及说明如下：

属性       | 类型     | 必需 | 说明
-----------|----------|----|--------
type       | `String` | 是 | 操作类型。这里是"Jdbc"。
saveAs     | `String` | 否 | 执行结果另存为一个新的变量的名称。变量的值是执行JDBC指定方法的返回值。
dataSource | `String` | 是 | 使用的数据源名称。
method     | `String` | 否 | 调用的JDBC方法。默认是"executeLargeUpdate"。
script     | `String` | 是 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。

目标JDBC SQL代码是在flink-jobs应用程序的main函数中运行的。

### 配置文件

默认的配置文件为flink-jobs.properties（注意：需在classpath下），可通过flink-jobs-context-loader.properties配置文件的`config.location`修改配置文件路径和名称。配置项的值允许通过占位符`${}`引用，例如`key=${anotherKey}`。

#### 数据源配置

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
```

#### Table API & SQL

[Flink](http://)的Table API & SQL配置除了在Flink配置文件中指定之外，也可以在flink-jobs的配置文件中指定。例如：

`table.exec.sink.not-null-enforcer=drop`

注意：如果是在flink-jobs的配置文件中配置这些参数，当执行自定义Java服务时，只有通过`FlinkJobsContext.getOrCreateStreamTableEnvironment()`或`FlinkJobsContext.getOrCreateStreamTableEnvironment(env)`方法获取的`StreamTableEnvironment`执行Table API & SQL，这些配置才会生效。

### 系统集成

[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)实现了使用XML配置文件来管理flink-jobs任务，这样开发Flink SQL任务会显得非常简单；同时，用户自定义的flink-jobs服务也可以被更轻松得集成到其他系统中。XML文件具有良好的可读性，并且在IDE环境下能够对配置进行自动提示。具体使用方法详见[flink-jobs-launcher开发文档](https://gitee.com/tenmg/flink-jobs-launcher)，以下介绍几种通过XML管理的flink-jobs任务：

#### 运行自定义服务

以下为一个自定义服务任务XML配置文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<flink-jobs xmlns="http://www.10mg.cn/schema/flink-jobs"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/flink-jobs http://www.10mg.cn/schema/flink-jobs.xsd"
	jar="/yourPath/yourJar.jar" serviceName="yourServiceName">
</flink-jobs>
```

#### 运行批处理SQL

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

#### 运行流处理SQL

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

### 发布计划

计划将在1.1.2中发布以下功能：

标签       | 功能     | 说明
-----------|---------|--------
`DataSync` | 数据同步 | 实现基于Debezuim的数据同步，以便简化通过`ExecuteSql`实现的数据同步功能。

### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request

### 相关链接

flink-jobs-launcher开源地址：https://gitee.com/tenmg/flink-jobs-launcher

DSL开源地址：https://gitee.com/tenmg/dsl

Flink官网：https://flink.apache.org

Debezuim官网：https://debezium.io