package cn.tenmg.flink.jobs.operator;

import java.lang.reflect.ParameterizedType;
import java.util.Map;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSON;

import cn.tenmg.flink.jobs.Operator;
import cn.tenmg.flink.jobs.model.Operate;

/**
 * 
 * 虚操作执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            操作类型
 * 
 * @since 1.1.0
 */
public abstract class AbstractOperator<T extends Operate> implements Operator {

	protected Class<T> type;

	@SuppressWarnings("unchecked")
	public AbstractOperator() {
		type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}

	@Override
	public void execute(StreamExecutionEnvironment env, String config, Map<String, Object> params) throws Exception {
		T operate = JSON.parseObject(config, type);
		String saveAs = operate.getSaveAs();
		if (saveAs != null) {
			params.put(saveAs, execute(env, JSON.parseObject(config, type), params));
		} else {
			execute(env, JSON.parseObject(config, type), params);
		}
	}

	/**
	 * 
	 * 执行操作
	 * 
	 * @param env
	 *            流运行环境
	 * @param operate
	 *            操作配置对象
	 * @param params
	 *            参数查找表
	 * @return 操作结果
	 * @throws Exception
	 *             发生异常
	 */
	public abstract Object execute(StreamExecutionEnvironment env, T operate, Map<String, Object> params) throws Exception;
}
