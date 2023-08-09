package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * {@code TIMESTAMP_LTZ} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class TimestampLtzDataTypeFactory extends ScalableDataTypeFactory {

	@Override
	DataType create(int scale) {
		return DataTypes.TIMESTAMP_LTZ(scale);
	}

	@Override
	DataType create() {
		return DataTypes.TIMESTAMP_LTZ();
	}

}
