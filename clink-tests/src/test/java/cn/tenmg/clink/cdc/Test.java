package cn.tenmg.clink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test {

	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		
		tableEnv.executeSql("CREATE TABLE orders (\r\n"
				+ "    id     INT,\r\n"
				+ "    name STRING,\r\n"
				+ "    PRIMARY KEY (id) NOT ENFORCED\r\n"
				+ ") WITH (\r\n"
				+ "    'connector' = 'oceanbase-cdc',\r\n"
				+ "    'scan.startup.mode' = 'initial',\r\n"
				+ "    'username' = 'root@sys#odb',\r\n"
				+ "    'password' = 'tenmg+10mg10MG',\r\n"
				+ "    'tenant-name' = 'sys',\r\n"
				+ "    'database-name' = 'test',\r\n"
				+ "    'table-name' = '^test_table1$',\r\n"
				+ "    'hostname' = '127.0.0.1',\r\n"
				+ "    'port' = '2881',\r\n"
				+ "    'rootserver-list' = '127.0.0.1:2882:2881',\r\n"
				+ "    'logproxy.host' = '127.0.0.1',\r\n"
				+ "    'logproxy.port' = '2983',\r\n"
				+ "    'working-mode' = 'memory'\r\n"
				+ ");");
		
		tableEnv.executeSql("select * from test_table1");
		
		
		/*ResolvedSchema resolvedSchema = new ResolvedSchema(
				Arrays.asList(Column.physical("id", DataTypes.INT().notNull()),
						Column.physical("name", DataTypes.STRING().notNull())),
				Collections.emptyList(), UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

		RowType physicalDataType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
		TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(physicalDataType);
		String serverTimeZone = "+00:00";	
		
		OceanBaseDeserializationSchema<RowData> deserializer = RowDataOceanBaseDeserializationSchema.newBuilder()
				.setPhysicalRowType(physicalDataType).setResultTypeInfo(resultTypeInfo)
				.setServerTimeZone(ZoneId.of(serverTimeZone)).build();		

		SourceFunction<RowData> oceanBaseSource = OceanBaseSource.<RowData>builder().rsList("127.0.0.1:2882:2881")
				.startupMode(StartupMode.INITIAL).username("root@sys").password("tenmg+10mg10MG")
				.tenantName("test_tenant").databaseName("^test_db$").tableName("^test_table1$").hostname("127.0.0.1")
				.port(2881).compatibleMode("mysql").jdbcDriver("com.mysql.jdbc.Driver").logProxyHost("127.0.0.1")
				.logProxyPort(2983).serverTimeZone(serverTimeZone).deserializer(deserializer).build();

		
		// enable checkpoint
		env.enableCheckpointing(3000);

		env.addSource(oceanBaseSource).print().setParallelism(1);
		env.execute("Print OceanBase Snapshot + Change Events");*/
	}

}
