package cn.tenmg.clink.cdc;

import cn.tenmg.clink.LocalTestSupported;

/**
 * MySQL到PostgresSQL多表数据同步
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年4月18日
 */
public class MySQLToPostgresSQLDataSyncTest extends LocalTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/cdc/mysql_to_postgresql.xml");
	}

}