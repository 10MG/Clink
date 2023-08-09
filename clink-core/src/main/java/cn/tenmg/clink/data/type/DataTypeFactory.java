package cn.tenmg.clink.data.type;

import org.apache.flink.table.types.DataType;

/**
 * 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public abstract interface DataTypeFactory {

	/**
	 * 获取适用的类型名称
	 * 
	 * @return 适用的类型名称
	 */
	String typeName();

	/**
	 * 判断类型描述是否支持
	 * 
	 * @param desc
	 *            类型描述
	 * @return 如支持该类型描述，则返回 {@code true}，否则，返回 {@code false}
	 */
	boolean supported(String desc);

	/**
	 * 根据类型描述生成数据类型 {@code DataType} 对象
	 * 
	 * @param desc
	 *            类型描述
	 * @return 数据类型 {@code DataType} 对象
	 */
	DataType create(String desc);

}
