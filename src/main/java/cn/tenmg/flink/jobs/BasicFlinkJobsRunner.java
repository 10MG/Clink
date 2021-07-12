package cn.tenmg.flink.jobs;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSON;

import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.Arguments;
import cn.tenmg.flink.jobs.utils.OperatorUtils;

/**
 * 
 * flink-jobs应用入口虚基类。使用该类，启动flink应用程序可获得flink-jobs封装的SQL等执行能力
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class BasicFlinkJobsRunner {

	/**
	 * 运行自定义程序
	 * 
	 * @param env
	 *            运行环境
	 * @param arguments
	 *            运行参数
	 * @throws Exception
	 *             发生异常
	 */
	protected abstract void run(StreamExecutionEnvironment env, Arguments arguments) throws Exception;

	/**
	 * 运行应用
	 * 
	 * @param args
	 *            运行参数
	 * @throws Exception
	 *             发生异常
	 */
	public void run(String... args) throws Exception {
		Arguments arguments;
		if (args == null || args.length < 1) {
			throw new IllegalArgumentException("You must provide a parameter in JSON format or the path of json file");
		} else if (args.length > 1) {
			throw new IllegalArgumentException(
					"Too many parameters. You must provide a parameter in JSON format or the path of json file");
		} else {
			String json = args[0];
			if (json.endsWith(".json")) {
				arguments = JSON.parseObject(new FileInputStream(json), Arguments.class);
			} else {
				arguments = JSON.parseObject(args[0], Arguments.class);
			}
			final StreamExecutionEnvironment env = FlinkJobsContext.getExecutionEnvironment();
			RuntimeExecutionMode mode = arguments.getRuntimeMode();
			// 设置运行模式
			if (RuntimeExecutionMode.BATCH.equals(mode)) {
				env.setRuntimeMode(RuntimeExecutionMode.BATCH);
			} else if (RuntimeExecutionMode.STREAMING.equals(mode)) {
				env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
			} else if (RuntimeExecutionMode.AUTOMATIC.equals(mode)) {
				env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
			}

			// 无参数则初始化空的参数查找表
			Map<String, Object> params = arguments.getParams();
			if (params == null) {
				params = new HashMap<String, Object>();
				arguments.setParams(params);
			}
			operates(env, arguments.getOperates(), params);// 获取和运行操作
			run(env, arguments);// 运行自定义处理
			FlinkJobsContext.remove();// 清除上下文缓存
		}
	}

	private void operates(final StreamExecutionEnvironment env, List<String> operates, Map<String, Object> params)
			throws Exception {
		if (operates != null) {
			String operate, type;
			for (int i = 0; i < operates.size(); i++) {
				operate = operates.get(i);
				type = JSON.parseObject(operate).getString("type");
				Operator operator = OperatorUtils.getOperator(type);
				if (operator == null) {
					throw new IllegalArgumentException("Operate of type " + type + " is not supported");
				}
				operator.execute(env, operate, params);
			}
		}
	}
}
