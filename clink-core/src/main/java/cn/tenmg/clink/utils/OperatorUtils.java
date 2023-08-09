package cn.tenmg.clink.utils;

import java.util.Map;

import cn.tenmg.clink.Operator;
import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.exception.IllegalConfigurationException;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 操作执行器工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class OperatorUtils {

	private static final String KEY_PREFIX = "operator" + ClinkContext.CONFIG_SPLITER;

	private static volatile Map<String, Operator> operators = MapUtils.newHashMap();

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
					String className = ClinkContext.getProperty(KEY_PREFIX + type);
					if (StringUtils.isBlank(className)) {
						throw new IllegalConfigurationException("Cannot find operator for operate of type " + type);
					} else {
						try {
							Operator operator = (Operator) Class.forName(className).getConstructor().newInstance();
							operators.put(type, operator);
							return operator;
						} catch (ClassNotFoundException e) {
							throw new IllegalConfigurationException(
									"Wrong operator configuration for operate of type " + type, e);
						} catch (Exception e) {
							throw new IllegalConfigurationException(
									"Cannot instantiate operator for operate of type " + type, e);
						}
					}
				}
			}
		}
	}

}
