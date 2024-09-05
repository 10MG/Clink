package cn.tenmg.clink.clients;

import cn.tenmg.clink.ClientsTestSupported;

/**
 * PostgresSQL到SQLServer多表数据同步
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年9月5日
 */
public class PostgresSQLToSQLServerDataSyncTest extends ClientsTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/cdc/postgresql_to_sqlserver.xml");
	}

}
