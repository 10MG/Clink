package cn.tenmg.clink.cdc;

import cn.tenmg.clink.LocalTestSupported;

/**
 * PostgresSQL到SQLServer多表数据同步
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年4月18日
 */
public class PostgresSQLToSQLServerDataSyncTest extends LocalTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/cdc/postgresql_to_sqlserver.xml");
	}

}
