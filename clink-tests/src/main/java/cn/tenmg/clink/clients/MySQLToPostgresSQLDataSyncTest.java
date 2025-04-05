package cn.tenmg.clink.clients;

import cn.tenmg.clink.ClientsTestSupported;

/**
 * MySQL到PostgresSQL多表数据同步
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年9月5日
 */
public class MySQLToPostgresSQLDataSyncTest extends ClientsTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/cdc/mysql_to_postgresql.xml");
	}

}