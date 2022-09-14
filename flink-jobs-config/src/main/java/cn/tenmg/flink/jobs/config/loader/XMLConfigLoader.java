package cn.tenmg.flink.jobs.config.loader;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.StringReader;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import cn.tenmg.flink.jobs.config.ConfigLoader;
import cn.tenmg.flink.jobs.config.model.FlinkJobs;

/**
 * XML配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
public class XMLConfigLoader implements ConfigLoader {

	private static final XMLConfigLoader INSTANCE = new XMLConfigLoader();

	private static Unmarshaller unmarshaller;

	static {
		try {
			unmarshaller = JAXBContext.newInstance(FlinkJobs.class).createUnmarshaller();
		} catch (JAXBException e) {
			e.printStackTrace();
		}
	}

	private XMLConfigLoader() {
		super();
	}

	public static final XMLConfigLoader getInstance() {
		return INSTANCE;
	}

	/**
	 * 加载flink-jobs配置
	 * 
	 * @param s
	 *            配置字符串
	 * @return flink-jobs配置模型
	 */
	public FlinkJobs load(String s) {
		try {
			return (FlinkJobs) unmarshaller.unmarshal(new StringReader(s));
		} catch (JAXBException e) {
			throw new IllegalArgumentException("Failed to load the flink-jobs configuration", e);
		}
	}

	/**
	 * 加载flink-jobs配置
	 * 
	 * @param file
	 *            配置文件
	 * @return flink-jobs配置模型
	 */
	public FlinkJobs load(File file) {
		try {
			return (FlinkJobs) unmarshaller.unmarshal(file);
		} catch (JAXBException e) {
			throw new IllegalArgumentException("Failed to load the flink-jobs configuration", e);
		}
	}

	/**
	 * 加载flink-jobs配置
	 * 
	 * @param fr
	 *            文件读取器
	 * @return flink-jobs配置模型
	 */
	public FlinkJobs load(FileReader fr) {
		try {
			return (FlinkJobs) unmarshaller.unmarshal(fr);
		} catch (JAXBException e) {
			throw new IllegalArgumentException("Failed to load the flink-jobs configuration", e);
		}
	}

	/**
	 * 加载flink-jobs配置
	 * 
	 * @param is
	 *            输入流
	 * @return flink-jobs配置模型
	 */
	@Override
	public FlinkJobs load(InputStream is) {
		try {
			return (FlinkJobs) unmarshaller.unmarshal(is);
		} catch (JAXBException e) {
			throw new IllegalArgumentException("Failed to load the flink-jobs configuration", e);
		}
	}
}
