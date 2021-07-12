package cn.tenmg.flink.jobs.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import cn.tenmg.flink.jobs.Operator;

/**
 * 操作执行器工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class OperatorUtils {

	private static final String OPERATOR = "Operator";

	private static final Map<String, Operator> operators = new HashMap<String, Operator>();

	static {
		ServiceLoader<Operator> loader = ServiceLoader.load(Operator.class);
		Operator operator;
		for (Iterator<Operator> it = loader.iterator(); it.hasNext();) {
			operator = it.next();
			operators.put(operator.getClass().getSimpleName().replace(OPERATOR, ""), operator);
		}
	}

	/**
	 * 根据操作类型获取操作执行器
	 * 
	 * @param type
	 *            操作类型
	 * @return 操作执行器或null
	 */
	public static Operator getOperator(String type) {
		return operators.get(type);
	}

}
