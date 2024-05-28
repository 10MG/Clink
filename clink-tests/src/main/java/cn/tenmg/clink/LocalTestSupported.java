package cn.tenmg.clink;

import com.alibaba.fastjson.JSON;

import cn.tenmg.clink.config.model.Clink;
import cn.tenmg.clink.context.ClinkContext;

/**
 * 使用本地环境运行的测试支持类
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年4月18日
 */
public abstract class LocalTestSupported extends XMLConfigTestSupported {

	protected static void test(String pathInClasspath) throws Exception {
		Clink clink = load(pathInClasspath);
		ClinkContext.getExecutionEnvironment(clink.getConfiguration()).setParallelism(1);// 最大并行度设置为1
		ClinkPortal.main(new String[] { JSON.toJSONString(clink) });
	}

}
