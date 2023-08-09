package cn.tenmg.clink.config;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;

import cn.tenmg.clink.config.model.Clink;

/**
 * 配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
public interface ConfigLoader {

	/**
	 * 加载Clink配置
	 * 
	 * @param s
	 *            配置字符串
	 * @return Clink配置模型
	 */
	Clink load(String s);

	/**
	 * 加载Clink配置
	 * 
	 * @param file
	 *            配置文件
	 * @return Clink配置模型
	 */
	Clink load(File file);

	/**
	 * 加载Clink配置
	 * 
	 * @param fr
	 *            文件读取器
	 * @return Clink配置模型
	 */
	Clink load(FileReader fr);

	/**
	 * 加载Clink配置
	 * 
	 * @param is
	 *            输入流
	 * @return Clink配置模型
	 */
	Clink load(InputStream is);
}
