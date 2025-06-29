package cn.tenmg.clink.datasync.kafka;

import cn.tenmg.clink.LocalTestSupported;

/**
 * Kafka的canal-json格式数据同步到MySQL
 * 
 * @author June wjzhao@aliyun.com
 * @since 2025年6月29日
 */
public class KafkaCanalJsonFormatToMySQLTest extends LocalTestSupported {

	public static void main(String[] args) throws Exception {
		test("cn/tenmg/clink/datasync/kafka/kafka-canal-json-format-to-mysql.xml");
	}

}
