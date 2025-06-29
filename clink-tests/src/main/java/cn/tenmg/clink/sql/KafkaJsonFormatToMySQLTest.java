package cn.tenmg.clink.sql;

import cn.tenmg.clink.LocalTestSupported;

public class KafkaJsonFormatToMySQLTest extends LocalTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/sql/kafka-json-format-to-mysql.xml");
	}

}
