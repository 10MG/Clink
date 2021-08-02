# flink-jobs

#### 介绍
flink-jobs为基于Flink的Java应用程序提供快速集成的能力，可通过继承FlinkJobsRunner快速构建基于SpringBoot的Flink流批一体应用程序。还可以通过使用[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)，实现基于Java API启动flink-jobs应用程序。

#### 起步

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

- 也可打包后，通过命令行提交给flink集群执行（通常在pom.xml配置org.apache.maven.plugins.shade.resource.ManifestResourceTransformer的mainClass为App这个类，请注意是完整类名）：`flink run /yourpath/yourfile.jar "{\"serviceName\":\"yourServiceName\"}"`，更多运行参数详见[运行参数](https://gitee.com/tenmg/flink-jobs/arguments.md)。

- 此外，通过使用[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)可以通过Java API的方式启动flink-jobs应用程序，这样启动操作就可以轻松集成到其他系统中（例如Java Web程序）。
