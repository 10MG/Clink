package cn.tenmg.clink;

import com.alibaba.fastjson.JSON;

import cn.tenmg.clink.config.loader.XMLConfigLoader;
import cn.tenmg.clink.config.model.Clink;
import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.dsl.utils.ClassUtils;

/**
 * 本地支持支持
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年4月18日
 */
public abstract class LocalTestSupported {

	protected static void test(String pathInClasspath) throws Exception {
		Clink clink = XMLConfigLoader.getInstance()
				.load(ClassUtils.getDefaultClassLoader().getResourceAsStream(pathInClasspath));
		ClinkContext.getExecutionEnvironment(clink.getConfiguration()).setParallelism(1);// 最大并行度设置为1
		ClinkPortal.main(new String[] { JSON.toJSONString(clink) });
	}

}
