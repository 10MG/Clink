package cn.tenmg.clink.clients;

import cn.tenmg.clink.ClientsTestSupported;

/**
 * Oracle到MySQL多表数据同步
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年4月19日
 */
public class OracleToMySQLDataSyncTest extends ClientsTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/cdc/oracle_to_mysql.xml");
	}

}
