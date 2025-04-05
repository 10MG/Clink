package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * {@code STRING} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class StringDataTypeFactory extends BasicDataTypeFactory {

	@Override
	DataType create() {
		return DataTypes.STRING();
	}

}
