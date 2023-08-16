package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * {@code TIME} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class TimeDataTypeFactory extends PrecisionDataTypeFactory {

	@Override
	DataType create(int precision) {
		return DataTypes.TIME(precision);
	}

	@Override
	DataType create() {
		return DataTypes.TIME();
	}

}
