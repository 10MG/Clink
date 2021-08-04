# flink-jobs

### 介绍
flink-jobs为基于Flink的Java应用程序提供快速集成的能力，可通过继承FlinkJobsRunner快速构建基于SpringBoot的Flink流批一体应用程序。还可以通过使用[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)，实现基于Java API启动flink-jobs应用程序。

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

- 也可打包后，通过命令行提交给flink集群执行（通常在pom.xml配置org.apache.maven.plugins.shade.resource.ManifestResourceTransformer的mainClass为App这个类，请注意是完整类名）：`flink run /yourpath/yourfile.jar "{\"serviceName\":\"yourServiceName\"}"`，更多运行参数详见[运行参数](#%E8%BF%90%E8%A1%8C%E5%8F%82%E6%95%B0)。

- 此外，通过使用[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)可以通过Java API的方式启动flink-jobs应用程序，这样启动操作就可以轻松集成到其他系统中（例如Java Web程序）。

### 运行参数
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

#### 运行参数相关属性及说明：

属性        | 类型   | 必需 | 说明
------------|--------|----|--------
serviceName | String | 否 | 运行的服务名称。该名称由用户定义并实现根据服务名称获取服务的方法，flink-jobs则在运行时调用并确定运行的实际服务。在运行SQL任务时，通常指定operates，而无需指定serviceName。
runtimeMode | String | 否 | 运行模式。可选值："BATCH"/"STREAMING"/"AUTOMATIC"，相关含义详见Flink官方文档。
params      | Object | 否 | 参数查找表。可用于SQL中。
operates    | Array  | 否 | 操作列表。目前支持类型为Bsh、ExecuteSql和SqlQuery三种操作。

##### Bsh操作

Bsh操作的作用是运行基于Beanshell的java代码，相关属性及说明如下：

属性   | 类型   | 必需 | 说明
-------|--------|----|--------
saveAs | String | 否 | 操作结果另存为一个新的变量的名称。变量的值是基于Beanshell的java代码的返回值（通过`return xxx;`表示）。
vars   | Array  | 否 | 参数声明列表。
java   | String | 否 | java代码。注意：使用泛型时，不能使用尖括号声明泛型。例如使用Map不能使用“Map<String , String> map = new HashMap<String , String>();”，但可以使用“Map map = new HashMap();”。

vars相关属性及说明如下：

属性   | 类型  | 必需 | 说明
------|--------|----|--------
name  | String | 是 | Beanshell中使用的变量名称
value | String | 否 | 变量对应的值的名称。默认与name相同。flink-jobs会从参数查找表中查找名称为value值的参数值，如果指定参数存在且不是null，则该值作为该参数的值；否则，使用value值作为该变量的值。

##### ExecuteSql操作

ExecuteSql操作的作用是运行基于DSL的SQL代码，相关属性及说明如下：

属性       | 类型  | 必需 | 说明
-----------|--------|----|--------
saveAs     | String | 否 | 操作结果另存为一个新的变量的名称。变量的值是flink的`tableEnv.executeSql(statement);`的返回值。
dataSource | String | 否 | 使用的数据源名称。
catalog    | String | 否 | 执行SQL使用的Flink SQL的catalog名称。
script     | String | 否 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。

##### SqlQuery操作

SqlQuery操作的作用是运行基于DSL的SQL查询代码，相关属性及说明如下：

属性       | 类型  | 必需 | 说明
-----------|--------|----|--------
saveAs     | String | 否 | 查询结果另存为临时表的表名及操作结果另存为一个新的变量的名称。变量的值是flink的`tableEnv.executeSql(statement);`的返回值。
catalog    | String | 否 | 执行SQL使用的Flink SQL的catalog名称。
script     | String | 否 | 基于[DSL](https://gitee.com/tenmg/dsl)的SQL脚本。