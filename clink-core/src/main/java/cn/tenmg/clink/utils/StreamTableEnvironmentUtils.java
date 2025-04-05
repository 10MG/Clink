package cn.tenmg.clink.utils;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import cn.tenmg.clink.context.ClinkContext;

/**
 * 流表环境工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.2
 */
public abstract class StreamTableEnvironmentUtils {

	/**
	 * 使用默认目录。
	 * 
	 * @param tableEnv 流表环境
	 */
	public static void useDefaultCatalog(StreamTableEnvironment tableEnv) {
		String currentCatalog = tableEnv.getCurrentCatalog();
		String defaultCatalog = ClinkContext.getDefaultCatalog(tableEnv);
		if (!defaultCatalog.equals(currentCatalog)) {
			tableEnv.useCatalog(defaultCatalog);
		}
	}

	/**
	 * 使用指定目录或者使用默认目录。即当指定目录为null时则切换至默认目录，否则切换至指定目录
	 * 
	 * @param tableEnv 流表环境
	 * @param catalog  指定目录
	 */
	public static void useCatalogOrDefault(StreamTableEnvironment tableEnv, String catalog) {
		if (catalog == null) {// 使用默认目录
			useDefaultCatalog(tableEnv);
		} else {// 使用自定义目录
			if (!catalog.equals(tableEnv.getCurrentCatalog())) {
				tableEnv.useCatalog(catalog);
			}
		}
	}

}
