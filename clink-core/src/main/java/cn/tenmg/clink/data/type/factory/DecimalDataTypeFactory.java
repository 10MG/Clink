package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

/**
 * {@code DECIMAL} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class DecimalDataTypeFactory extends AccurateDataTypeFactory {

	@Override
	DataType create(int scale, int precision) {
		return DataTypes.DECIMAL(precision, scale);
	}

	@Override
	DataType create(int scale) {
		return DataTypes.DECIMAL(DecimalType.DEFAULT_PRECISION, scale);
	}

	@Override
	DataType create() {
		return DataTypes.DECIMAL(DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE);
	}

}
