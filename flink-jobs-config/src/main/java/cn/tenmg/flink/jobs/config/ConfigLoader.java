package cn.tenmg.flink.jobs.config;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;

import cn.tenmg.flink.jobs.config.model.FlinkJobs;

/**
 * 配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
public interface ConfigLoader {

	/**
	 * 加载flink-jobs配置
	 * 
	 * @param s
	 *            配置字符串
	 * @return flink-jobs配置模型
	 */
	FlinkJobs load(String s);

	/**
	 * 加载flink-jobs配置
	 * 
	 * @param file
	 *            配置文件
	 * @return flink-jobs配置模型
	 */
	FlinkJobs load(File file);

	/**
	 * 加载flink-jobs配置
	 * 
	 * @param fr
	 *            文件读取器
	 * @return flink-jobs配置模型
	 */
	FlinkJobs load(FileReader fr);

	/**
	 * 加载flink-jobs配置
	 * 
	 * @param is
	 *            输入流
	 * @return flink-jobs配置模型
	 */
	FlinkJobs load(InputStream is);
}
