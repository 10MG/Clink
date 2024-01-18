# clink-clients

<p align="left">
    <a href="https://mvnrepository.com/artifact/cn.tenmg/clink-clients">
        <img alt="maven" src="https://img.shields.io/maven-central/v/cn.tenmg/clink-clients.svg?style=flat-square">
    </a>
    <a target="_blank" href="LICENSE"><img src="https://img.shields.io/:license-Apache%202.0-blue.svg"></a>
</p>

## 介绍
clink-clients是[Clink](https://gitee.com/tenmg/clink)应用程序客户端类库，可用于启动、监控和停止Clink或普通flink作业，通过clink-clients可将flink快速集成到现有基于Java实现的系统中，还可以通过XML格式的配置文件玩转Flink SQL。一个典型的clink-clients部署架构如下：

![典型的clink-clients部署架构](../%E5%85%B8%E5%9E%8B%E6%9E%B6%E6%9E%84.png)

## 起步

以下以Maven项目为例介绍StandaloneRestClusterClient的使用：

### 添加依赖

```
<!-- https://mvnrepository.com/artifact/cn.tenmg/clink-clients -->
<dependency>
    <groupId>cn.tenmg</groupId>
    <artifactId>clink-clients</artifactId>
    <version>${clink-clients.version}</version>
</dependency>
```

### 添加配置

clink.properties

```
# REST 配置，用“,”分隔不同的地址使用“:”分隔地址和端口号，端口号可省略默认为8081
rest.addresses=192.168.100.11,192.168.100.12,192.168.100.13
# 或者也允许使用 rest.address
# rest.address=192.168.100.11,192.168.100.12,192.168.100.13
# 只重试一次（默认值为20），以避免某些节点挂了后重试时间过长
rest.retry.max-attempts=1

# Clink客户端提交执行的默认 jar，它不是必需的，可以是项目中的任何一个主类
# clink.default.jar=/yourpath/your-clink-app-1.0.0.jar
# Clink客户端提交执行的默认类名，它不是必需的，默认为 cn.tenmg.clink.ClinkPortal，也可以实现和配置自己的类或者在 jar 中指定主类
# clink.default.class=cn.tenmg.clink.ClinkPortal
```

### 提交作业

调用XMLConfigLoader的load方法加载XML配置文件并提交给客户端执行：

```
Clink clink = XMLConfigLoader.getInstance().load(ClassUtils.getDefaultClassLoader().getResourceAsStream("clink.xml"));
StandaloneRestClusterClient client = new StandaloneRestClusterClient();
JobID jobId = client.submit(clink);
System.out.println("Flink job launched: " + jobId.toHexString());// 启动Clink作业
```

或

```
Clink clink = XMLConfigLoader.getInstance()
	.load("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n" + 
		"<clink xmlns=\"http://www.10mg.cn/schema/clink\"\r\n" + 
		"	xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\r\n" + 
		"	xsi:schemaLocation=\"http://www.10mg.cn/schema/clink http://www.10mg.cn/schema/clink.xsd\"\r\n" + 
		"	jar=\"/opt/clink/clink-quickstart-1.1.4.jar\" serviceName=\"HelloWorldService\">\r\n" + 
       		"</clink>");
StandaloneRestClusterClient client = new StandaloneRestClusterClient();
JobID jobId = client.submit(clink);
System.out.println("Flink job launched: " + jobId.toHexString());// 启动Clink作业
```

### 监控状态


```
JobID jobId = JobID.fromHexString(hexString);
JobStatus jobStatus = client.getJobStatus(jobId);// 获取作业状态
System.out.println("Job status: " + jobStatus);
```

### 高级功能

```
//ClusterClient clusterClient = client.getClusterClient(customConf);// 使用自定义配置获取ClusterClient
ClusterClient clusterClient = client.getClusterClient();
// Use clusterClient to do something
```

### 停止作业


```
System.out.println("Flink job of jobId: " + jobId.toHexString() + " stopped, savepoint path: " + client.stop(jobId));clink-jobs作业
	
```

## 任务配置

### `<clink>`

clink是Clink任务XML配置文件的根节点，需注意必须配置正确的命名空间，通常结构如下：

```
<clink xmlns="http://www.10mg.cn/schema/clink"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/clink http://www.10mg.cn/schema/clink.xsd">
</clink>
```

相关属性及说明：

属性        | 类型                | 必需 | 说明
------------|----------------------|----|--------
jar         | `String`             | 否 | 运行的JAR包。可通过配置文件的`clink.default.jar`配置指定默认运行的JAR包。
class       | `String`             | 否 | 运行的主类。可通过配置文件的`clink.default.class`配置指定默认运行的主类。
serviceName | `String`             | 否 | 运行的服务名称。该名称由用户定义并实现根据服务名称获取服务的方法，[Clink](https://gitee.com/tenmg/Clink)则在运行时调用并确定运行的实际服务。在运行SQL任务时，通常通过clink内的其他标签（如`<execute-sql>`）指定操作，而无需指定serviceName。
runtimeMode | `String`             | 否 | 运行模式。可选值："BATCH"/"STREAMING"/"AUTOMATIC"，相关含义详见[Flink](https://flink.apache.org)官方文档。

#### `<configuration>`

Flink作业的个性化配置，格式为`k1=v1[,k2=v3…]`。例如：`<configuration><![CDATA[pipeline.name=customJobName]]></configuration>`表示自定义Flink SQL作业的名称为`customJobName`。具体配置项详见[Flink官方文档](https://flink.apache.org/)。

#### `<options>`

运行选项配置，用于指定flink程序的运行选项。

##### `<option>`

特定运行选项配置。XSD文件提供了选项key值的枚举，能够在IDE环境下自动提示。

![自动补全样例](AutomaticCompletionExample.png)

属性  | 类型     | 必需 | 说明
------|----------|----|--------
key   | `String` | 是 | 选项键。
value | `String` | 否 | 选项的值。使用标签内文本表示，如`<option>value</option>`或`<option><![CDATA[value]]></option>`。

#### `<params>`

参数查找表配置。通常可用于SQL中，也可以在[Clink](https://gitee.com/tenmg/Clink)应用程序自定义的服务中通过arguments参数获取。

##### `<param>`

特定参数配置。

属性  | 类型     | 必需 | 说明
------|----------|----|--------
name  | `String` | 是 | 参数名。
value | `String` | 否 | 参数值。使用标签内文本表示。

#### `<bsh>`

运行基于Beanshell的java代码的配置。

属性   | 类型     | 必需 | 说明
-------|----------|----|--------
saveAs | `String` | 否 | 操作结果另存为一个新的变量的名称。变量的值是基于Beanshell的java代码的返回值（通过`return xxx;`表示）。
when   | `String` | 否 | 操作的条件，当且仅当该条件满足时，才执行该操作。不指定时，默认表示条件成立。

##### `<var>`

基于Beanshell的java代码使用的变量声明配置。

属性   | 类型  | 必需 | 说明
------|--------|----|--------
name  | `String` | 是 | Beanshell中使用的变量名称
value | `String` | 否 | 变量对应的值的名称。默认与name相同。[Clink](https://gitee.com/tenmg/Clink)会从参数查找表中查找名称为value值的参数值，如果指定参数存在且不是null，则该值作为该参数的值；否则，使用value值作为该变量的值。

##### `<java>`

java代码。采用文本表示，如：`<java>java code</java>`或`<option><![CDATA[java code]]></option>`。注意：使用泛型时，不能使用尖括号声明泛型。例如，使用Map不能使用“Map<String , String> map = new HashMap<String , String>();”，但可以使用“Map map = new HashMap();”。

#### `<execute-sql>`

运行基于[DSL](https://gitee.com/tenmg/dsl)的SQL代码配置。

属性             | 类型     | 必需 | 说明
-----------------|----------|----|--------
saveAs           | `String` | 否 | 操作结果另存为一个新的变量的名称。变量的值是flink的`tableEnv.executeSql(statement);`的返回值。
when             | `String` | 否 | 操作的条件，当且仅当该条件满足时，才执行该操作。不指定时，默认表示条件成立。
dataSource       | `String` | 否 | 使用的数据源名称。详见[Clink数据源配置](https://gitee.com/tenmg/Clink#%E6%95%B0%E6%8D%AE%E6%BA%90%E9%85%8D%E7%BD%AE)。
dataSourceFilter | `String` | 否 | 使用的数据源过滤器。内置两种数据源过滤器（source/sink），如果内置过滤器无法满足使用要求，也可使用自定义类名（该类需实现`cn.tenmg.clink.datasource.DataSourceFilter`接口）。
catalog          | `String` | 否 | 执行SQL使用的Flink SQL的catalog名称。
script           | `String` | 否 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。使用标签内文本表示，如：`<execute-sql>SQL code</execute-sql>`或`<execute-sql><![CDATA[SQL code]]></execute-sql>`。由于Flink SQL不支持DELETE、UPDATE语句，因此如果配置的SQL脚本是DELETE或者UPDATE语句，该语句将在程序main函数中采用JDBC执行。

#### `<sql-query>`

运行基于[DSL](https://gitee.com/tenmg/dsl)的SQL查询代码配置。

属性       | 类型     | 必需 | 说明
-----------|----------|----|--------
saveAs     | `String` | 否 | 查询结果另存为临时表的表名及操作结果另存为一个新的变量的名称。变量的值是flink的`tableEnv.executeSql(statement);`的返回值。
when       | `String` | 否 | 操作的条件，当且仅当该条件满足时，才执行该操作。不指定时，默认表示条件成立。
catalog    | `String` | 否 | 执行SQL使用的Flink SQL的catalog名称。
script     | `String` | 否 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。使用标签内文本表示，如：`<sql-query>SQL code</sql-query>`或`<sql-query><![CDATA[SQL code]]></sql-query>`。

#### `<jdbc>`

运行基于[DSL](https://gitee.com/tenmg/dsl)的JDBC SQL代码配置。目标JDBC SQL代码是在[Clink](https://gitee.com/tenmg/Clink)应用程序的main函数中运行的。

属性       | 类型     | 必需 | 说明
-----------|----------|----|--------
saveAs     | `String` | 否 | 执行结果另存为一个新的变量的名称。变量的值是执行JDBC指定方法的返回值。
when       | `String` | 否 | 操作的条件，当且仅当该条件满足时，才执行该操作。不指定时，默认表示条件成立。
dataSource | `String` | 是 | 使用的数据源名称。详见[Clink数据源配置](https://gitee.com/tenmg/Clink#%E6%95%B0%E6%8D%AE%E6%BA%90%E9%85%8D%E7%BD%AE)。
method     | `String` | 否 | 调用的JDBC方法，支持"get"/"select"/"execute"/"executeUpdate"/"executeLargeUpdate"，默认是"executeUpdate"（1.4.0及之前版本默认值为"executeLargeUpdate"，由于很多数据库连接池或者JDBC驱动未实现该方法，因此1.4.1版本开始改为"executeUpdate"）。可在配置文件中使用`jdbc.default-method`配置项修改默认值。
script     | `String` | 是 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。使用标签内文本表示。

#### `<data-sync>`

运行基于Flink SQL的流式任务实现数据同步。相关属性及说明如下：

属性       | 类型      | 必需 | 说明
-----------|-----------|----|--------
saveAs     | `String`  | 否 | 执行结果另存为一个新的变量的名称。变量的值是执行`INSERT`语句返回的`org.apache.flink.table.api.TableResult`对象。一般不使用。
when       | `String`  | 否 | 操作的条件，当且仅当该条件满足时，才执行该操作。不指定时，默认表示条件成立。
from       | `String`  | 是 | 来源数据源名称。目前仅支持Kafka数据源。
topic      | `String`  | 否 | Kafka主题。也可在fromConfig中配置`topic=xxx`。
fromConfig | `String`  | 否 | 来源配置。例如：`properties.group.id=Clink`。
to         | `String`  | 是 | 目标数据源名称，目前仅支持JDBC数据源。
toConfig   | `String`  | 是 | 目标配置。例如：`sink.buffer-flush.max-rows = 0`。
table      | `String`  | 是 | 同步数据表名。
primaryKey | `String`  | 否 | 主键，多个列名以“,”分隔。当开启智能模式时，会自动获取主键信息。
timestamp  | `String`  | 否 | 时间戳列名，多个列名使用“,”分隔。设置这个值后，创建源表和目标表时会添加这些列，并在数据同步时写入这些列。一般在Clink应用程序中使用配置文件统一指定，而不是每个同步任务单独指定。
smart      | `Boolean` | 否 | 是否开启智能模式。不设置时，根据全局配置确定是否开启智能模式，全局默认配置为`clink.smart=true`。
`<column>` | `Element` | 否 | 同步数据列。当开启智能模式时，会自动获取列信息。

##### `<column>`

属性     | 类型     | 必需 | 说明
---------|----------|----|--------
fromName | `String` | 是 | 来源列名。
fromType | `String` | 否 | 来源数据类型。如果缺省，则如果开启智能模式会自动获取目标数据类型作为来源数据类型，如果关闭智能模式则必填。
toName   | `String` | 否 | 目标列名。默认为来源列名。
toType   | `String` | 否 | 目标列数据类型。如果缺省，则如果开启智能模式会自动获取，如果关闭智能模式则默认为来源列数据类型。
strategy | `String` | 否 | 同步策略。可选值：both/from/to，both表示来源列和目标列均创建，from表示仅创建原来列，to表示仅创建目标列，默认为both。
script   | `String` | 否 | 自定义脚本。通常是需要进行函数转换时使用。使用标签内文本表示。

#### `<create-table>`

根据指定的配置信息自动生成Fink SQL并创建一张表。这比手动拼写Flink SQL要高效很多。支持版本：1.3.0+，相关属性及说明如下：

属性             | 类型     | 必需 | 说明
-----------------|----------|----|--------
saveAs           | `String` | 否 | 操作结果另存为一个新的变量的名称。变量的值是flink的`tableEnv.executeSql(statement);`的返回值。
when             | `String` | 否 | 操作的条件，当且仅当该条件满足时，才执行该操作。不指定时，默认表示条件成立。
dataSource       | `String` | 是 | 使用的数据源名称。Clink从该数据源读取元数据信息，并自动生成Flink SQL。
dataSourceFilter | `String` | 否 | 使用的数据源过滤器。内置两种数据源过滤器（source/sink），如果内置过滤器无法满足使用要求，也可使用自定义类名（该类需实现`cn.tenmg.clink.datasource.DataSourceFilter`接口）。
tableName        | `String` | 是 | 创建表的表名。即`CREATE TABLE table_name ...`中的`table_name`。
catalog          | `String` | 否 | 执行SQL使用的Flink SQL的catalog名称。
bindTableName    | `String` | 否 | 绑定的表名，即WITH子句的“table-name”，默认与tableName相同。
primaryKey       | `String` | 否 | 主键，多个列名以“,”分隔。当开启智能模式时，会自动获取主键信息。
smart            | `String` | 否 | 是否开启智能模式。不设置时，根据Clink应用程序的全局配置确定是否开启智能模式，Clink应用程序的全局默认配置为`clink.smart=true`。

##### Column

列信息配置。开启智能模式时，一般不需要配置，Clink会自动生成列及对应的数据类型。但也可以单独指定某些列的数据类型，不使用自动识别的类型。

属性 | 类型     | 必需 | 说明
-----|----------|----|--------
name | `String` | 是 | 列名。
type | `String` | 是 | 数据类型。使用标签内文本表示。


### XML配置示例

为了更好的理解Clink的XML配置文件，以下提供几种常见场景的XML配置文件示例：

#### 运行普通flink程序

```
<?xml version="1.0" encoding="UTF-8"?>
<clink xmlns="http://www.10mg.cn/schema/clink"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/clink http://www.10mg.cn/schema/clink.xsd"
	jar="D:\Programs\flink-1.8.3\examples\batch\WordCount.jar">
</clink>
```

#### 运行自定义服务

以下为一个自定义服务任务XML配置文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<clink xmlns="http://www.10mg.cn/schema/clink"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/clink http://www.10mg.cn/schema/clink.xsd"
	jar="/yourPath/yourJar.jar" serviceName="yourServiceName">
</clink>
```

#### 运行批处理SQL

以下为一个简单订单量统计SQL批处理任务XML配置文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<clink xmlns="http://www.10mg.cn/schema/clink"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/clink http://www.10mg.cn/schema/clink.xsd"
	jar="/yourPath/yourJar.jar">
	<!--任务运行参数，一些公共参数也可在调用Java API之前指定，例如系统时间等 -->
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
</clink>
```

#### 运行流处理SQL

以下为通过Debezium实现异构数据库同步任务XML配置文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<clink xmlns="http://www.10mg.cn/schema/clink"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/clink http://www.10mg.cn/schema/clink.xsd">
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
	<!-- 定义名为kafka数据源的订单明细表 -->
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
		) WITH ('topic' = 'kaorder1.kaorder.order_detail', 'properties.group.id' = 'Clink_source_order_detail')
		]]>
	</execute-sql>
	<!-- 定义名为source数据源的订单明细表 -->
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
	<!-- 将kafka订单明细数据插入到source数据库订单明细表中 -->
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
</clink>
```

#### 运行数据同步任务

以下为通过Debezium实现异构数据库同步任务XML配置文件：

```
<?xml version="1.0" encoding="UTF-8"?>
<clink xmlns="http://www.10mg.cn/schema/clink"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:schemaLocation="http://www.10mg.cn/schema/clink http://www.10mg.cn/schema/clink-1.1.2.xsd">
	<data-sync table="od_order_info" to="data_skyline"
		from="kafka" topic="testdb.testdb.od_order_info">
		<!-- 在数据源和目标库表结构相同（字段名及类型均相同）的情况下，智能模式可自动从目标库获取表元数据信息，只要少量配就能完成数据同步。 -->
		<!-- 在数据源和目标库表结构不同（字段名或类型不同）的情况，需要自定义列的差异信息，例如自定来源类型和转换函数： -->
		<column fromName="UPDATE_TIME" fromType="BIGINT">TO_TIMESTAMP(FROM_UNIXTIME(UPDATE_TIME/1000, 'yyyy-MM-dd HH:mm:ss'))</column>
		<!-- 另外，如果关闭智能模式，需要列出所有列的详细信息。 -->
	</data-sync>
</clink>
```

## 配置文件

默认的配置文件为`clink.properties`（注意：需在`classpath`下），也可在实例化客户端实现类时指定。如：

```
StandaloneRestClusterClient client = new StandaloneRestClusterClient("my.properties");
```

### 通用配置

属性                     | 类型     | 必需 | 说明
-------------------------|----------|----|--------
clink.default.jar   | `String` | 否 | 启动时默认向Flink提交的JAR包。即当任务配置中没有指定`jar`时，会采用此配置。
clink.default.class | `String` | 否 | 启动时默认向Flink提交运行的主类。即当任务配置中没有指定`class`时，会采用此配置。

### StandaloneRestClusterClient

属性                     | 类型     | 必需 | 说明
-------------------------|----------|----|--------
jobmanager.rpc.servers   | `String` | 是 | Flink集群远程调用地址，格式为`host1:port1,host2:port2,…`，其中端口号(`port`)可以缺省，缺省时所有端口号均为`jobmanager.rpc.port`的值。
jobmanager.rpc.address   | `String` | 否 | Flink集群远程调用地址，只能配置一个主机地址，已不推荐使用。配置`jobmanager.rpc.servers`后，该配置失效。
jobmanager.rpc.port      | `int`    | 否 | Flink集群远程调用端口号，默认值为`6123`。
jobmanager.*             |    略    | 略 | Flink集群的其他配置参见[Flink官网](https://flink.apache.org)对应版本的文档。
rest.*                   |    略    | 略 | Flink集群的其他配置参见[Flink官网](https://flink.apache.org)对应版本的文档。

## 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request

## 相关链接

Clink开源地址：https://gitee.com/tenmg/Clink

DSL开源地址：https://gitee.com/tenmg/dsl

Flink官网：https://flink.apache.org

Debezuim官网：https://debezium.io
