package cn.tenmg.clink.source.factory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.mysql.source.config.ServerIdRange;
import com.ververica.cdc.connectors.mysql.table.StartupMode;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import com.ververica.cdc.debezium.table.MetadataConverter;
import com.ververica.cdc.debezium.utils.JdbcUrlUtils;

import cn.tenmg.clink.cdc.debezium.MultiTableDebeziumDeserializationSchema;
import cn.tenmg.clink.source.SourceFactory;
import cn.tenmg.clink.utils.SQLUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.StringUtils;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;

/**
 * 基于 Flink CDC MySQL 连接器的支持多表的源工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class FlinkCdcMySqlSourceFactory implements SourceFactory<MySqlSource<Tuple2<String, Row>>> {

	public static final String INCLUDE_SCHEMA_CHANGES = "include-schema-changes", DATABASE_NAME = "database-name";

	@Override
	public MySqlSource<Tuple2<String, Row>> create(Map<String, String> config, Map<String, RowType> rowTypes,
			Map<Integer, String> metadatas) {
		String databaseName = config.get(MySqlSourceOptions.DATABASE_NAME.key()), parts[];
		Set<String> databases = new HashSet<String>(), tables;
		if (StringUtils.isBlank(databaseName)) {
			tables = rowTypes.keySet();
			for (String tableName : tables) {
				parts = tableName.split("\\.", 2);
				if (parts.length > 1) {
					databases.add(parts[0]);
				}
			}
		} else {
			if (databaseName.startsWith(SQLUtils.SINGLE_QUOTATION_MARK)
					&& databaseName.endsWith(SQLUtils.SINGLE_QUOTATION_MARK)) {
				databaseName = databaseName.substring(1, databaseName.length() - 1);
			}
			databases.add(databaseName);
			tables = new HashSet<String>();
			for (String tableName : rowTypes.keySet()) {
				parts = tableName.split("\\.", 2);
				if (parts.length > 1) {
					databases.add(parts[0]);
					tables.add(tableName);
				} else {
					tables.add(StringUtils.concat(databaseName, ".", tableName));
				}
			}
		}

		MySqlSourceBuilder<Tuple2<String, Row>> builder = MySqlSource.<Tuple2<String, Row>>builder()
				.hostname(config.get(MySqlSourceOptions.HOSTNAME.key()))
				.port(getIntegerOrDefault(config, MySqlSourceOptions.PORT))
				.username(config.get(MySqlSourceOptions.USERNAME.key()))
				.password(config.get(MySqlSourceOptions.PASSWORD.key())).databaseList(String.join(",", databases))
				.tableList(String.join(",", tables)).serverId(validateAndGetServerId(config))
				.debeziumProperties(DebeziumOptions.getDebeziumProperties(config))
				.jdbcProperties(JdbcUrlUtils.getJdbcProperties(config));

		StartupOptions startupOptions = getStartupOptions(config);
		builder.startupOptions(startupOptions);
		if (config.containsKey(MySqlSourceOptions.SERVER_TIME_ZONE.key())) {
			builder.serverTimeZone(config.get(MySqlSourceOptions.SERVER_TIME_ZONE.key()));
		}
		if (getBooleanOrDefault(config, MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED)) {
			validateStartupOptionIfEnableParallel(startupOptions);

			int splitSize = getIntegerOrDefault(config, MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
			validateIntegerOption(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
			builder.splitSize(splitSize);

			int fetchSize = getIntegerOrDefault(config, MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
			validateIntegerOption(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
			builder.fetchSize(fetchSize);

			int splitMetaGroupSize = getIntegerOrDefault(config, MySqlSourceOptions.CHUNK_META_GROUP_SIZE);
			validateIntegerOption(MySqlSourceOptions.CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
			builder.splitMetaGroupSize(splitMetaGroupSize);

			int connectionPoolSize = getIntegerOrDefault(config, MySqlSourceOptions.CONNECTION_POOL_SIZE);
			validateIntegerOption(MySqlSourceOptions.CONNECTION_POOL_SIZE, connectionPoolSize, 1);
			builder.connectionPoolSize(connectionPoolSize);

			int connectMaxRetries = getIntegerOrDefault(config, MySqlSourceOptions.CONNECT_MAX_RETRIES);
			validateIntegerOption(MySqlSourceOptions.CONNECT_MAX_RETRIES, connectMaxRetries, 0);
			builder.connectMaxRetries(connectMaxRetries);

			builder.distributionFactorLower(Double
					.parseDouble(getOrDefault(config, Arrays.asList("chunk-key.even-distribution.factor.lower-bound",
							"split-key.even-distribution.factor.lower-bound"), "0.05")));
			builder.distributionFactorUpper(Double
					.parseDouble(getOrDefault(config, Arrays.asList("chunk-key.even-distribution.factor.upper-bound",
							"split-key.even-distribution.factor.upper-bound"), "1000")));
			builder.connectTimeout(getDurationOrDefault(config, MySqlSourceOptions.CONNECT_TIMEOUT));
			builder.scanNewlyAddedTableEnabled(
					getBooleanOrDefault(config, MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED));
			builder.heartbeatInterval(getDurationOrDefault(config, MySqlSourceOptions.HEARTBEAT_INTERVAL));
			if (config.containsKey(INCLUDE_SCHEMA_CHANGES)) {
				builder.includeSchemaChanges(Boolean.valueOf(config.get(INCLUDE_SCHEMA_CHANGES)));
			}
		}
		return builder
				.deserializer(new MultiTableDebeziumDeserializationSchema(rowTypes, toMetadataConverters(metadatas)))
				.build();
	}

	private static String getOrDefault(Map<String, String> config, ConfigOption<String> option) {
		if (config.containsKey(option.key())) {
			return config.get(option.key());
		}
		return option.defaultValue();
	}

	private static Integer getIntegerOrDefault(Map<String, String> config, ConfigOption<Integer> option) {
		if (config.containsKey(option.key())) {
			return Integer.parseInt(config.get(option.key()));
		}
		return option.defaultValue();
	}

	private static String getOrDefault(Map<String, String> config, List<String> priorityKeys, String defaultValue) {
		String key;
		for (int i = 0, size = priorityKeys.size(); i < size; i++) {
			key = priorityKeys.get(i);
			if (config.containsKey(key)) {
				return config.get(key);
			}
		}
		return defaultValue;
	}

	private static Boolean getBooleanOrDefault(Map<String, String> config, ConfigOption<Boolean> option) {
		if (config.containsKey(option.key())) {
			return Boolean.parseBoolean(config.get(option.key()));
		}
		return option.defaultValue();
	}

	private static Duration getDurationOrDefault(Map<String, String> config, ConfigOption<Duration> option) {
		if (config.containsKey(option.key())) {
			return TimeUtils.parseDuration(config.get(option.key()));
		}
		return option.defaultValue();
	}

	private String validateAndGetServerId(Map<String, String> config) {
		final String serverIdValue = config.get(MySqlSourceOptions.SERVER_ID.key());
		if (serverIdValue != null) {
			// validation
			try {
				ServerIdRange.from(serverIdValue);
			} catch (Exception e) {
				throw new ValidationException(
						String.format("The value of option 'server-id' is invalid: '%s'", serverIdValue), e);
			}
		}
		return serverIdValue;
	}

	private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
	private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
	private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
	private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
	private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

	private static StartupOptions getStartupOptions(Map<String, String> config) {
		String modeString = getOrDefault(config, MySqlSourceOptions.SCAN_STARTUP_MODE);
		switch (modeString.toLowerCase()) {
		case SCAN_STARTUP_MODE_VALUE_INITIAL:
			return StartupOptions.initial();

		case SCAN_STARTUP_MODE_VALUE_LATEST:
			return StartupOptions.latest();

		case SCAN_STARTUP_MODE_VALUE_EARLIEST:
		case SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET:
		case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
			throw new ValidationException(String.format(
					"Unsupported option value '%s', the options [%s, %s, %s] are not supported correctly, please do not use them until they're correctly supported",
					modeString, SCAN_STARTUP_MODE_VALUE_EARLIEST, SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET,
					SCAN_STARTUP_MODE_VALUE_TIMESTAMP));

		default:
			throw new ValidationException(
					String.format("Invalid value for option '%s'. Supported values are [%s, %s], but was: %s",
							MySqlSourceOptions.SCAN_STARTUP_MODE.key(), SCAN_STARTUP_MODE_VALUE_INITIAL,
							SCAN_STARTUP_MODE_VALUE_LATEST, modeString));
		}
	}

	private void validateStartupOptionIfEnableParallel(StartupOptions startupOptions) {
		Preconditions.checkState(
				startupOptions.startupMode == StartupMode.INITIAL
						|| startupOptions.startupMode == StartupMode.LATEST_OFFSET,
				String.format("MySql Parallel Source only supports startup mode 'initial' and 'latest-offset',"
						+ " but actual is %s", startupOptions.startupMode));
	}

	private void validateIntegerOption(ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
		Preconditions.checkState(optionValue > exclusiveMin, String.format(
				"The value of option '%s' must larger than %d, but is %d", option.key(), exclusiveMin, optionValue));
	}

	private static final Map<String, MetadataConverter> METADATA_CONVERTERS = MapUtils
			.newHashMapBuilder(String.class, MetadataConverter.class).put("table_name", new MetadataConverter() {
				private static final long serialVersionUID = 1L;

				@Override
				public Object read(SourceRecord record) {
					Struct messageStruct = (Struct) record.value();
					Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
					return StringData.fromString(sourceStruct.getString(AbstractSourceInfo.TABLE_NAME_KEY));
				}
			}).put("database_name", new MetadataConverter() {
				private static final long serialVersionUID = 1L;

				@Override
				public Object read(SourceRecord record) {
					Struct messageStruct = (Struct) record.value();
					Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
					return StringData.fromString(sourceStruct.getString(AbstractSourceInfo.DATABASE_NAME_KEY));
				}
			}).put("op_ts", new MetadataConverter() {
				private static final long serialVersionUID = 1L;

				@Override
				public Object read(SourceRecord record) {
					Struct messageStruct = (Struct) record.value();
					Struct sourceStruct = messageStruct.getStruct(Envelope.FieldName.SOURCE);
					return TimestampData.fromEpochMillis((Long) sourceStruct.get(AbstractSourceInfo.TIMESTAMP_KEY));
				}
			}).put("op", new MetadataConverter() {
				private static final long serialVersionUID = 1L;

				@Override
				public Object read(SourceRecord record) {
					Envelope.Operation op = Envelope.operationFor(record);
					if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
						return StringData.fromString("c");
					} else if (op == Envelope.Operation.DELETE) {
						return StringData.fromString("d");
					} else {
						return StringData.fromString("u");
					}
				}
			}).build();

	private Map<Integer, MetadataConverter> toMetadataConverters(Map<Integer, String> metadatas) {
		Map<Integer, MetadataConverter> metadataConverters = MapUtils.newHashMap();
		if (metadatas == null) {
			return null;
		}
		Entry<Integer, String> entry;
		MetadataConverter metadataConverter;
		for (Iterator<Entry<Integer, String>> it = metadatas.entrySet().iterator(); it.hasNext();) {
			entry = it.next();
			metadataConverter = METADATA_CONVERTERS.get(entry.getValue());
			if (metadataConverter == null) {
				throw new UnsupportedOperationException("Invalid metadata key: " + entry.getKey());
			}
			metadataConverters.put(entry.getKey(), metadataConverter);
		}
		return metadataConverters;
	}

}
