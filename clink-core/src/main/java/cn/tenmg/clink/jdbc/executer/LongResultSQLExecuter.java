package cn.tenmg.clink.jdbc.executer;

/**
 * 返回<code>java.lang.Long</code>查询结果类型的SQL执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.4.0
 */
public class LongResultSQLExecuter extends GetSQLExecuter<Long> {

	private static final LongResultSQLExecuter INSTANCE = new LongResultSQLExecuter();

	private LongResultSQLExecuter() {
		super();
	}

	public static final LongResultSQLExecuter getInstance() {
		return INSTANCE;
	}

}
