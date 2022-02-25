package cn.tenmg.flink.jobs;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import cn.tenmg.flink.jobs.context.FlinkJobsContext;

public class FlinkSQLTest {
	
	public static void main(String[] args) {
		StreamTableEnvironment env = FlinkJobsContext.getOrCreateStreamTableEnvironment();
		env.executeSql("CREATE TABLE OD_ORDER_INFO (\r\n" + 
				"				ORDER_ID STRING NOT NULL,\r\n" + 
				"				ORDER_NO STRING,\r\n" + 
				"				ORGAN_ID STRING,\r\n" + 
				"				PROCTIME  AS PROCTIME(),\r\n" + 
				"				PRIMARY KEY (ORDER_ID) NOT ENFORCED\r\n" + 
				"			)\r\n" + 
				"			WITH ('topic' = 'test.test.od_order_info', 'properties.group.id' = 'flink-jobs-fact_order_info', 'properties.bootstrap.servers' = '192.168.100.22:9092,192.168.100.23:9092,192.168.100.24:9092,192.168.100.25:9092,192.168.100.26:9092', 'connector' = 'kafka', 'debezium-json.schema-include' = 'false', 'format' = 'debezium-json', 'scan.startup.mode' = 'earliest-offset')");
		
		env.executeSql("CREATE TABLE SYS_ORGAN_INFO (\r\n" + 
				"				ORGAN_ID STRING NOT NULL,\r\n" + 
				"				ORGAN_NAME STRING NOT NULL,\r\n" + 
				"				PRIMARY KEY (ORGAN_ID) NOT ENFORCED\r\n" + 
				"			) WITH ('driver' = 'com.mysql.cj.jdbc.Driver', 'connector' = 'jdbc', 'url' = 'jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull', 'password' = '', 'username' = 'root', 'table-name' = 'SYS_ORGAN_INFO')");
		
		env.executeSql("CREATE TABLE FACT_ORDER_INFO (\r\n" + 
				"				ORDER_ID STRING NOT NULL,\r\n" + 
				"				ORGAN_ID STRING,\r\n" + 
				"				ORGAN_NAME STRING,\r\n" + 
				"				PRIMARY KEY (ORDER_ID) NOT ENFORCED\r\n" + 
				"			) WITH ('driver' = 'com.mysql.cj.jdbc.Driver', 'connector' = 'jdbc', 'url' = 'jdbc:mysql://127.0.0.1:3306/test?useSSL=false&serverTimezone=GMT%2B8&zeroDateTimeBehavior=convertToNull', 'password' = '', 'username' = 'root', 'table-name' = 'FACT_ORDER_INFO')");
		
		env.executeSql("INSERT INTO FACT_ORDER_INFO(\r\n" + 
				"				ORDER_ID,\r\n" + 
				"				ORGAN_ID,\r\n" + 
				"				ORGAN_NAME\r\n" + 
				"			)\r\n" + 
				"			SELECT\r\n" + 
				"				O.ORDER_ID,\r\n" + 
				"				O.ORGAN_ID,\r\n" + 
				"				G.ORGAN_NAME\r\n" + 
				"			FROM OD_ORDER_INFO O\r\n" + 
				"			LEFT JOIN SYS_ORGAN_INFO FOR SYSTEM_TIME AS OF O.PROCTIME AS G\r\n" + 
				"			  ON O.ORGAN_ID = G.ORGAN_ID");	
	}
}
