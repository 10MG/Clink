package cn.tenmg.flink.jobs.parser;

import java.sql.Time;
import java.sql.Timestamp;

import cn.tenmg.dsl.parser.PlaintextParamsParser;
import cn.tenmg.flink.jobs.utils.DateUtils;

/**
 * FlinkSQL参数解析器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.2
 */
public class FlinkSQLParamsParser extends PlaintextParamsParser {

	private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss", TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.S",
			TIME_PATTERN = "HH:mm:ss";

	private static final FlinkSQLParamsParser INSTANCE = new FlinkSQLParamsParser();

	protected FlinkSQLParamsParser() {
		super();
	}

	public static FlinkSQLParamsParser getInstance() {
		return INSTANCE;
	}

	@Override
	protected String convert(Object value) {
		String s;
		if (value instanceof Timestamp) {
			s = "TO_TIMESTAMP('" + DateUtils.format(value, TIMESTAMP_PATTERN) + "', '" + TIMESTAMP_PATTERN + "')";
		} else if (value instanceof Time) {
			s = "TIME('" + DateUtils.format(value, TIME_PATTERN) + "', '" + TIME_PATTERN + "')";
		} else {
			s = "TO_DATE('" + DateUtils.format(value, DATE_PATTERN) + "', '" + DATE_PATTERN + "')";
		}
		return s;
	}
}
