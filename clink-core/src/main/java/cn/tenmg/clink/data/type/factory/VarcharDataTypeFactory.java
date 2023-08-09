package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * {@code VARCHAR} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class VarcharDataTypeFactory extends ScalableDataTypeFactory {

	@Override
	DataType create(int scale) {
		return DataTypes.VARCHAR(scale);
	}

	@Override
	DataType create() {
		return DataTypes.VARCHAR(VarCharType.DEFAULT_LENGTH);
	}

}
