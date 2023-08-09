package cn.tenmg.clink;

import cn.tenmg.clink.runner.SimpleClinkRunner;

/**
 * 默认Clink程序入口
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.2
 * 
 */
public class ClinkPortal {

	public static void main(String... args) throws Exception {
		SimpleClinkRunner.getInstance().run(args);
	}

}
