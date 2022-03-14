package cn.tenmg.flink.jobs.utils;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import cn.tenmg.flink.jobs.context.FlinkJobsContext;

/**
 * 流表环境工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.3.0
 */
public abstract class StreamTableEnvironmentUtils {

	/**
	 * 使用指定目录或者使用默认目录。即当指定目录为null时则切换至默认目录，否则切换至指定目录
	 * 
	 * @param tableEnv
	 *            流表环境
	 * @param catalog
	 *            指定目录
	 */
	public static void useCatalogOrDefault(StreamTableEnvironment tableEnv, String catalog) {
		String currentCatalog = tableEnv.getCurrentCatalog();
		if (catalog == null) {// 使用默认目录
			String defaultCatalog = FlinkJobsContext.getDefaultCatalog(tableEnv);
			if (!defaultCatalog.equals(currentCatalog)) {
				tableEnv.useCatalog(defaultCatalog);
			}
		} else {// 使用自定义目录
			if (!catalog.equals(currentCatalog)) {
				tableEnv.useCatalog(catalog);
			}
		}
	}

}
