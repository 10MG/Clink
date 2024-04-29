package cn.tenmg.clink.cdc;

import cn.tenmg.clink.LocalTestSupported;

/**
 * SQLServer到Oracle多表数据同步
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年4月19日
 */
public class SQLServerToOracleDataSyncTest extends LocalTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/cdc/sqlserver_to_oracle.xml");
	}

}
