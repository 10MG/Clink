package cn.tenmg.clink.table.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * 多表数据行匹配收集函数
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class MultiTablesRowsFilterFunction implements FlatMapFunction<Tuple2<String, Row>, Row> {

	private static final long serialVersionUID = -5871149385805088526L;

	private final String tableName;

	@Override
	public void flatMap(Tuple2<String, Row> value, Collector<Row> out) throws Exception {
		if (value.f0.equals(tableName)) {
			out.collect(value.f1);
		}
	}

	public MultiTablesRowsFilterFunction(String tableName) {
		super();
		this.tableName = tableName;
	}

}
