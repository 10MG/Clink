package cn.tenmg.clink.operator;

import java.lang.reflect.ParameterizedType;
import java.util.Map;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.alibaba.fastjson2.JSON;

import cn.tenmg.clink.Operator;
import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.model.SqlQuery;
import cn.tenmg.clink.utils.StreamTableEnvironmentUtils;

/**
 * 抽象SQL操作执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            操作类型
 * 
 * @since 1.1.0
 */
public abstract class AbstractSqlOperator<T extends SqlQuery> implements Operator {

	protected Class<T> type;

	@SuppressWarnings("unchecked")
	public AbstractSqlOperator() {
		type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}

	@Override
	public void execute(StreamExecutionEnvironment env, String config, Map<String, Object> params) throws Exception {
		StreamTableEnvironment tableEnv = ClinkContext.getOrCreateStreamTableEnvironment(env);
		T operate = JSON.parseObject(config, type);
		StreamTableEnvironmentUtils.useCatalogOrDefault(tableEnv, operate.getCatalog());
		String saveAs = operate.getSaveAs();
		if (saveAs == null) {
			execute(tableEnv, JSON.parseObject(config, type), params);
		} else {
			params.put(saveAs, execute(tableEnv, JSON.parseObject(config, type), params));
		}
	}

	/**
	 * 
	 * 执行操作
	 * 
	 * @param tableEnv
	 *            流表环境
	 * @param operate
	 *            操作配置对象
	 * @param params
	 *            参数查找表
	 * @return 操作结果
	 * @throws Exception
	 *             发生异常
	 */
	abstract Object execute(StreamTableEnvironment tableEnv, T operate, Map<String, Object> params) throws Exception;

}
