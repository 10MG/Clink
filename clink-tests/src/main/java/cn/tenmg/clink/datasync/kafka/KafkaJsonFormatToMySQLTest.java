package cn.tenmg.clink.datasync.kafka;

import cn.tenmg.clink.LocalTestSupported;

/**
 * Kafka的JSON格式数据同步到MySQL
 * 
 * @author June wjzhao@aliyun.com
 * @since 2025年6月29日
 */
public class KafkaJsonFormatToMySQLTest extends LocalTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/datasync/kafka/kafka-json-format-to-mysql.xml");
	}

}
