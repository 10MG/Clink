package cn.tenmg.flink.jobs.jdbc.executor;

/**
 * 返回<code>java.lang.Long</code>查询结果类型的SQL执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.4.0
 */
public class LongResultSQLExecutor extends GetSQLExecutor<Long> {

	private static final LongResultSQLExecutor INSTANCE = new LongResultSQLExecutor();

	private LongResultSQLExecutor() {
		super();
	}

	public static final LongResultSQLExecutor getInstance() {
		return INSTANCE;
	}

}
