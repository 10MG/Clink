package cn.tenmg.flink.jobs.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cn.tenmg.flink.jobs.Operator;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;

/**
 * 操作执行器工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class OperatorUtils {

	private static final String KEY_PREFIX = "operator.";

	private static volatile Map<String, Operator> operators = new HashMap<String, Operator>();

	/**
	 * 根据操作类型获取操作执行器
	 * 
	 * @param type
	 *            操作类型
	 * @return 操作执行器
	 */
	public static Operator getOperator(String type) {
		if (operators.containsKey(type)) {
			return operators.get(type);
		} else {
			synchronized (operators) {
				if (operators.containsKey(type)) {
					return operators.get(type);
				} else {
					String className = FlinkJobsContext.getProperty(KEY_PREFIX + type);
					if (className == null) {
						throw new IllegalArgumentException("Operate of type " + type + " is not supported");
					} else if (StringUtils.isBlank(className)) {
						throw new IllegalArgumentException("Cannot find operator for operate of type " + type);
					} else {
						try {
							Operator operator = (Operator) Class.forName(className).newInstance();
							operators.put(type, operator);
							return operator;
						} catch (InstantiationException | IllegalAccessException e) {
							throw new IllegalArgumentException(
									"Cannot instantiate operator for operate of type " + type, e);
						} catch (ClassNotFoundException e) {
							throw new IllegalArgumentException(
									"Wrong operator configuration for operate of type " + type, e);
						}
					}
				}
			}
		}
	}

}
