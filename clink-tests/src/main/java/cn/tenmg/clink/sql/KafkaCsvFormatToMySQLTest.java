package cn.tenmg.clink.sql;

import cn.tenmg.clink.LocalTestSupported;

public class KafkaCsvFormatToMySQLTest extends LocalTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/sql/kafka-csv-format-to-mysql.xml");
	}

}