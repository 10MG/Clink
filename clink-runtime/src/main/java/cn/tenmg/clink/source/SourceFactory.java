package cn.tenmg.clink.source;

import java.util.Map;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

/**
 * 支持多表的源工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public interface SourceFactory<S extends Source<Tuple2<String, Row>, ?, ?>> {

	/**
	 * 获取工厂唯一标识
	 * 
	 * @return 工厂唯一标识
	 */
	String factoryIdentifier();

	/**
	 * 创建源
	 * 
	 * @param config
	 *            源的配置
	 * @param rowTypes
	 *            行类型查找表
	 * @param metadatas
	 *            元数据查找表
	 * @return 源
	 */
	S create(Map<String, String> config, Map<String, RowType> rowTypes, Map<String, Map<Integer, String>> metadatas);

}
