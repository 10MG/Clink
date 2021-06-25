# flink-jobs

#### 介绍
flink-jobs为基于Flink的Java应用程序提供快速集成的能力，可通过继承FlinkJobsRunner快速构建基于SpringBoot的Flink流批一体应用程序。还可以通过使用[flink-jobs-launcher](https://gitee.com/tenmg/flink-jobs-launcher)，实现基于Java API启动flink-jobs应用程序。

#### 使用说明

以基于SpringBoot的Maven项目为例

1.  pom.xml添加依赖，${flink-jobs.version}为版本号，可定义属性或直接使用版本号替换

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
group.id.prefix=consumer_group_flink_jobs
defaultService=helloWordService
defaultRuntimeMode=BATCH
```

3.  编写配置类
```
@Configuration
@PropertySource(value = "application.properties")
public class Context {

	@Bean
	public Properties kafkaProperties(@Value("${bootstrap.servers}") String servers,
			@Value("${group.id.prefix}") String groupIdPrefix, @Value("${auto.offset.reset}") String autoOffsetReset) {
		Properties kafkaProperties = new Properties();
		kafkaProperties.put("bootstrap.servers", servers);
		kafkaProperties.put("group.id.prefix", groupIdPrefix);
		kafkaProperties.put("auto.offset.reset", autoOffsetReset);
		return kafkaProperties;
	}

}
```

4.  编写应用入口类

```
@ComponentScan("cn.tenmg.flink.jobs")
public class App extends FlinkJobsRunner implements CommandLineRunner {

	@Value("${defaultService}")
	private String defaultService;

	@Value("${defaultRuntimeMode}")
	private String defaultRuntimeMode;

	@Autowired
	private ApplicationContext springContext;

	@Override
	protected String getDefaultService() {
		return defaultService;
	}

	@Override
	protected RuntimeExecutionMode getDefaultRuntimeMode() {
		return RuntimeExecutionMode.valueOf(defaultRuntimeMode);
	}

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
public class HelloWordService implements StreamService {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6651233695630282701L;

	@Autowired
	private Properties kafkaProperties;

	@Value("${topics}")
	private String topics;

	@Override
	public void run(StreamExecutionEnvironment env, Params params) throws Exception {
		DataStream<String> stream;
		if (RuntimeExecutionMode.STREAMING.equals(params.getRuntimeMode())) {
			Properties properties = new Properties();
			properties.putAll(kafkaProperties);
			properties.setProperty("group.id", kafkaProperties.getProperty("group.id.prefix").concat("helloword"));
			properties.remove("group.id.prefix");
			stream = env.addSource(new FlinkKafkaConsumer011<String>(Arrays.asList(topics.split(",")),
					new SimpleStringSchema(), kafkaProperties));
		} else {
			stream = env.fromElements("Hello, World!");
		}
		stream.print();
	}

}
```
