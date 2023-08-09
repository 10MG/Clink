package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * 
 * {@code MULTISET} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class MultisetDataTypeFactory extends NestedDataTypeFactory<DataType> {

	@Override
	DataType create(DataType elementDataType) {
		return DataTypes.MULTISET(elementDataType);
	}

}
