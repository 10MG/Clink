package cn.tenmg.clink.datasync.cdc;

import cn.tenmg.clink.LocalTestSupported;

/**
 * Oracle到MySQL多表数据同步
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年4月19日
 */
public class OracleToMySQLDataSyncTest extends LocalTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/datasync/cdc/oracle_to_mysql.xml");
	}

}
