package cn.tenmg.flink.jobs.clients;

import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.PropertyFilter;

import cn.tenmg.flink.jobs.FlinkJobsClient;
import cn.tenmg.flink.jobs.clients.context.FlinkJobsClientsContext;
import cn.tenmg.flink.jobs.clients.utils.Sets;
import cn.tenmg.flink.jobs.config.model.FlinkJobs;

/**
 * flink-jobs客户端抽象类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 */
public abstract class AbstractFlinkJobsClient<T> implements FlinkJobsClient<T> {

	private static final String FLINK_JOBS_DEFAULT_JAR_KEY = "flink.jobs.default.jar",
			FLINK_JOBS_DEFAULT_CLASS_KEY = "flink.jobs.default.class", JAR = ".jar";

	private static final Set<String> EXCLUDES = Sets.as("options", "mainClass", "jar", "allwaysNewJob");

	protected static final String EMPTY_ARGUMENTS = "{}";

	/**
	 * 获取运行的JAR。如果flink-jobs配置对象没有配置运行的JAR则返回配置文件中配置的默认JAR，如果均没有，则返回<code>null</code>
	 * 
	 * @param flinkJobs
	 *            flink-jobs配置对象
	 * @return 返回运行的JAR
	 */
	protected static String getJar(FlinkJobs flinkJobs) {
		String jar = flinkJobs.getJar();
		if (jar == null) {
			jar = FlinkJobsClientsContext.getProperty(FLINK_JOBS_DEFAULT_JAR_KEY);
		}
		if (jar == null) {
			throw new IllegalArgumentException("You must specify the jar to run or configurate by key "
					+ FLINK_JOBS_DEFAULT_JAR_KEY + " in " + FlinkJobsClientsContext.getConfigLocation());
		}
		return jar;
	}

	/**
	 * 验证JAR文件后缀是否合法
	 * 
	 * @param jar
	 *            JAR文件
	 * @return 如果合法则返回JAR文件后缀的开始位置，否则将抛出异常
	 */
	protected static int validateJar(String jar) {
		int jarIndex = jar.lastIndexOf(JAR);
		if (jarIndex <= 0) {
			throw new IllegalArgumentException("The jar file configuration must end with " + JAR + " suffix");
		}
		return jarIndex;
	}

	/**
	 * 获取入口类名
	 * 
	 * @param flinkJobs
	 *            flink-jobs配置对象
	 * @return 返回入口类名
	 */
	protected static String getEntryPointClassName(FlinkJobs flinkJobs) {
		String mainClass = flinkJobs.getMainClass();
		if (mainClass == null) {
			mainClass = FlinkJobsClientsContext.getProperty(FLINK_JOBS_DEFAULT_CLASS_KEY);
		}
		return mainClass;
	}

	/**
	 * 获取flink程序运行参数
	 * 
	 * @param flinkJobs
	 *            flink-jobs配置对象
	 * @return 返回运行
	 */
	protected static String getArguments(FlinkJobs flinkJobs) {
		return JSON.toJSONString(flinkJobs, new PropertyFilter() {
			@Override
			public boolean apply(Object object, String name, Object value) {
				if (EXCLUDES.contains(name)) {// 排除在外的字段
					return false;
				}
				return true;
			}
		});
	}

	/**
	 * 判断运行参数是否为空
	 * 
	 * @param arguments
	 *            运行参数
	 * @return true/false
	 */
	protected static Boolean isEmptyArguments(String arguments) {
		return arguments == null || "{}".equals(arguments) || "".equals(arguments.trim());
	}

}
