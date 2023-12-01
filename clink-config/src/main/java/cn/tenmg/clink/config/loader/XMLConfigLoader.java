package cn.tenmg.clink.config.loader;

import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.StringReader;

import cn.tenmg.clink.config.ConfigLoader;
import cn.tenmg.clink.config.model.Clink;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;

/**
 * XML配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
public class XMLConfigLoader implements ConfigLoader {

	private static final XMLConfigLoader INSTANCE = new XMLConfigLoader();

	private static volatile JAXBContext context;

	private XMLConfigLoader() {
		super();
	}

	public static final XMLConfigLoader getInstance() {
		return INSTANCE;
	}

	/**
	 * 加载Clink配置
	 * 
	 * @param s 配置字符串
	 * @return Clink配置模型
	 */
	public Clink load(String s) {
		try {
			return (Clink) newUnmarshaller().unmarshal(new StringReader(s));
		} catch (JAXBException e) {
			throw new IllegalArgumentException("Failed to load the Clink configuration", e);
		}
	}

	/**
	 * 加载Clink配置
	 * 
	 * @param file 配置文件
	 * @return Clink配置模型
	 */
	public Clink load(File file) {
		try {
			return (Clink) newUnmarshaller().unmarshal(file);
		} catch (JAXBException e) {
			throw new IllegalArgumentException("Failed to load the Clink configuration", e);
		}
	}

	/**
	 * 加载Clink配置
	 * 
	 * @param fr 文件读取器
	 * @return Clink配置模型
	 */
	public Clink load(FileReader fr) {
		try {
			return (Clink) newUnmarshaller().unmarshal(fr);
		} catch (JAXBException e) {
			throw new IllegalArgumentException("Failed to load the Clink configuration", e);
		}
	}

	/**
	 * 加载Clink配置
	 * 
	 * @param is 输入流
	 * @return Clink配置模型
	 */
	@Override
	public Clink load(InputStream is) {
		try {
			return (Clink) newUnmarshaller().unmarshal(is);
		} catch (JAXBException e) {
			throw new IllegalArgumentException("Failed to load the Clink configuration", e);
		}
	}

	private Unmarshaller newUnmarshaller() throws JAXBException {
		if (context == null) {
			context = JAXBContext.newInstance(Clink.class);
		}
		return context.createUnmarshaller();
	}
}
