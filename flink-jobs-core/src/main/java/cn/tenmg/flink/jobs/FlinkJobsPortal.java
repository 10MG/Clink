package cn.tenmg.flink.jobs;

import cn.tenmg.flink.jobs.runner.SimpleFlinkJobsRunner;

/**
 * 默认flink-jobs程序入口
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 * 
 */
public class FlinkJobsPortal {

	public static void main(String[] args) throws Exception {
		SimpleFlinkJobsRunner.getInstance().run(args);
	}

}
