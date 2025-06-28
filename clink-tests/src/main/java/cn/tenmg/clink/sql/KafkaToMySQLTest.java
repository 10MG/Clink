package cn.tenmg.clink.sql;

import cn.tenmg.clink.LocalTestSupported;

public class KafkaToMySQLTest extends LocalTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/sql/kafka-to-mysql.xml");
	}

}
