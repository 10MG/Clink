package cn.tenmg.clink;

import cn.tenmg.clink.config.loader.XMLConfigLoader;
import cn.tenmg.clink.config.model.Clink;
import cn.tenmg.dsl.utils.ClassUtils;

/**
 * 支持XML配置进行测试的支持类
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年5月28日
 */
public abstract class XMLConfigTestSupported {

	protected static Clink load(String pathInClasspath) {
		return XMLConfigLoader.getInstance()
				.load(ClassUtils.getDefaultClassLoader().getResourceAsStream(pathInClasspath));
	}

}
