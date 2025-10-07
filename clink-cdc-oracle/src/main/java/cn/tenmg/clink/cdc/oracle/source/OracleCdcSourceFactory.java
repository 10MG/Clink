package cn.tenmg.clink.cdc.oracle.source;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupMode;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.oracle.source.OracleSourceBuilder;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.cdc.debezium.table.MetadataConverter;

import cn.tenmg.clink.cdc.oracle.debezium.MultiTableDebeziumDeserializationSchema;
import cn.tenmg.clink.source.SourceFactory;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.SetUtils;
import cn.tenmg.dsl.utils.StringUtils;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.data.Envelope;

/**
 * 基于 Oracle CDC 连接器的支持多表的源工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.1
 */
public class OracleCdcSourceFactory implements SourceFactory<JdbcIncrementalSource<Tuple2<String, Row>>> {

	public static final String IDENTIFIER = "oracle-cdc";

	private static final String TABLE_NAME = "table-name", INCLUDE_SCHEMA_CHANGES = "include-schema-changes",
			CONVERT_DELETE_TO_UPDATE = "convert-delete-to-update",
			SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN = JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN
					.key(),
			SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED = "scan.incremental.close-idle-reader.enabled",
			CLOSE_IDLE_READERS_METHOD_NAME = "closeIdleReaders",
			SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP = "scan.incremental.snapshot.backfill.skip",
			SKIP_SNAPSHOT_BACKFILL_METHOD_NAME = "skipSnapshotBackfill";

	private static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname").stringType().noDefaultValue()
			.withDescription("IP address or hostname of the Oracle database server.");

	private static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().defaultValue(1433)
			.withDescription("Integer port number of the Oracle database server.");

	private static final ConfigOption<String> USERNAME = ConfigOptions.key("username").stringType().noDefaultValue()
			.withDescription("Name of the Oracle database to use when connecting to the Oracle database server.");

	private static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue()
			.withDescription("Password to use when connecting to the Oracle database server.");

	private static final ConfigOption<String> DATABASE_NAME = ConfigOptions.key("database-name").stringType()
			.noDefaultValue().withDescription("Database name of the Oracle server to monitor.");

	private static final ConfigOption<String> SCAN_STARTUP_MODE = ConfigOptions.key("scan.startup.mode").stringType()
			.defaultValue("initial")
			.withDescription("Optional startup mode for Oracle CDC consumer, valid enumerations are "
					+ "\"initial\", \"initial-only\", \"latest-offset\"");

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public JdbcIncrementalSource<Tuple2<String, Row>> create(Map<String, String> config, Map<String, RowType> rowTypes,
			Map<String, Map<Integer, String>> metadatas) {
		String schemaNames = getOrDefault(config, JdbcSourceOptions.SCHEMA_NAME);
		Set<String> schemas = schemaNames == null ? Collections.emptySet()
				: SetUtils.newHashSet(trimAll(schemaNames.split(","))), tables = new HashSet<String>();
		String[] tableNames = toArray(config.get(TABLE_NAME)), parts;
		if (tableNames == null) {
			for (String tableName : rowTypes.keySet()) {
				parts = tableName.split("\\.", 2);
				if (parts.length > 1 || schemas.isEmpty()) {
					tables.add(tableName);
				} else {
					for (Iterator<String> it = schemas.iterator(); it.hasNext();) {
						tables.add(StringUtils.concat(it.next(), ".", tableName));
					}
				}
			}
		} else {
			for (String tableName : tableNames) {
				parts = tableName.split("\\.", 2);
				if (parts.length > 1 || schemas.isEmpty()) {
					tables.add(tableName);
				} else {
					for (Iterator<String> it = schemas.iterator(); it.hasNext();) {
						tables.add(StringUtils.concat(it.next(), ".", tableName));
					}
				}
			}
		}
		OracleSourceBuilder<Tuple2<String, Row>> builder = OracleSourceBuilder.OracleIncrementalSource
				.<Tuple2<String, Row>>builder().hostname(getOrDefault(config, HOSTNAME))
				.port(getIntegerOrDefault(config, PORT)).username(getOrDefault(config, USERNAME))
				.password(getOrDefault(config, PASSWORD)).databaseList(toArray(getOrDefault(config, DATABASE_NAME)))
				.schemaList(toArray(schemas)).tableList(toArray(tables))
				.debeziumProperties(DebeziumOptions.getDebeziumProperties(config));

		StartupOptions startupOptions = getStartupOptions(config);
		validateStartupOption(startupOptions);
		builder.startupOptions(startupOptions);

		int splitSize = getIntegerOrDefault(config, SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
		validateIntegerOption(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
		builder.splitSize(splitSize);
		int fetchSize = getIntegerOrDefault(config, SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
		validateIntegerOption(SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
		builder.fetchSize(fetchSize);

		int splitMetaGroupSize = getIntegerOrDefault(config, SourceOptions.CHUNK_META_GROUP_SIZE);
		validateIntegerOption(SourceOptions.CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
		builder.splitMetaGroupSize(splitMetaGroupSize);

		int connectionPoolSize = getIntegerOrDefault(config, JdbcSourceOptions.CONNECTION_POOL_SIZE);
		validateIntegerOption(JdbcSourceOptions.CONNECTION_POOL_SIZE, connectionPoolSize, 1);
		builder.connectionPoolSize(connectionPoolSize);

		int connectMaxRetries = getIntegerOrDefault(config, JdbcSourceOptions.CONNECT_MAX_RETRIES);
		validateIntegerOption(JdbcSourceOptions.CONNECT_MAX_RETRIES, connectMaxRetries, 0);
		builder.connectMaxRetries(connectMaxRetries);

		builder.distributionFactorLower(
				Double.parseDouble(getOrDefault(config, Arrays.asList("chunk-key.even-distribution.factor.lower-bound",
						"split-key.even-distribution.factor.lower-bound"), "0.05")));
		builder.distributionFactorUpper(
				Double.parseDouble(getOrDefault(config, Arrays.asList("chunk-key.even-distribution.factor.upper-bound",
						"split-key.even-distribution.factor.upper-bound"), "1000")));
		builder.connectTimeout(getDurationOrDefault(config, JdbcSourceOptions.CONNECT_TIMEOUT));
		if (config.containsKey(INCLUDE_SCHEMA_CHANGES)) {
			builder.includeSchemaChanges(Boolean.valueOf(config.get(INCLUDE_SCHEMA_CHANGES)));
		}
		if (config.containsKey(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN)) {
			builder.chunkKeyColumn(config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN));
		}
		if (config.containsKey(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED)) {
			setbooleanOptionIfPossible(builder, CLOSE_IDLE_READERS_METHOD_NAME,
					Boolean.valueOf(config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED)));
		}
		if (config.containsKey(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP)) {
			setbooleanOptionIfPossible(builder, SKIP_SNAPSHOT_BACKFILL_METHOD_NAME,
					Boolean.valueOf(config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP)));
		}

		String convertDeleteToUpdate = config.get(CONVERT_DELETE_TO_UPDATE);
		return builder
				.deserializer(new MultiTableDebeziumDeserializationSchema(rowTypes, toMetadataConverters(metadatas),
						convertDeleteToUpdate == null ? false : Boolean.parseBoolean(convertDeleteToUpdate)))
				.build();
	}

	@Override
	public Set<String> metadataKeys() {
		return METADATA_CONVERTERS.keySet();
	}

	private static String[] toArray(Set<String> strs) {
		return strs.toArray(new String[strs.size()]);
	}

	private static String[] toArray(String str) {
		if (str == null) {
			return null;
		}
		return trimAll(str.split(","));
	}

	private static String[] trimAll(String[] strs) {
		for (int i = 0; i < strs.length; i++) {
			strs[i] = strs[i].trim();
		}
		return strs;
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

	private static Duration getDurationOrDefault(Map<String, String> config, ConfigOption<Duration> option) {
		if (config.containsKey(option.key())) {
			return TimeUtils.parseDuration(config.get(option.key()));
		}
		return option.defaultValue();
	}

	private void setbooleanOptionIfPossible(Object obj, String name, boolean value) {
		try {
			obj.getClass().getDeclaredMethod(name, boolean.class).invoke(obj, value);
		} catch (Exception e) {
		}
	}

	private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
	private static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest-offset";
	private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
	private static final String SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET = "specific-offset";
	private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

	private static StartupOptions getStartupOptions(Map<String, String> config) {
		String modeString = getOrDefault(config, SCAN_STARTUP_MODE);
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
							SCAN_STARTUP_MODE.key(), SCAN_STARTUP_MODE_VALUE_INITIAL, SCAN_STARTUP_MODE_VALUE_LATEST,
							modeString));
		}
	}

	private void validateStartupOption(StartupOptions startupOptions) {
		Preconditions.checkState(
				startupOptions.startupMode == StartupMode.INITIAL
						|| startupOptions.startupMode == StartupMode.LATEST_OFFSET,
				String.format("PostgreSQL Parallel Source only supports startup mode 'initial' and 'latest-offset',"
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

	private Map<String, Map<Integer, MetadataConverter>> toMetadataConverters(
			Map<String, Map<Integer, String>> metadatas) {
		if (MapUtils.isEmpty(metadatas)) {
			return Collections.emptyMap();
		}
		Map<String, Map<Integer, MetadataConverter>> metadataConverterses = MapUtils.newHashMap();
		Entry<String, Map<Integer, String>> entry;
		Map<Integer, MetadataConverter> metadataConverters;
		for (Iterator<Entry<String, Map<Integer, String>>> it = metadatas.entrySet().iterator(); it.hasNext();) {
			entry = it.next();
			Entry<Integer, String> e;
			MetadataConverter metadataConverter;
			metadataConverters = MapUtils.newHashMap();
			for (Iterator<Entry<Integer, String>> eit = entry.getValue().entrySet().iterator(); eit.hasNext();) {
				e = eit.next();
				metadataConverter = METADATA_CONVERTERS.get(e.getValue());
				if (metadataConverter == null) {
					throw new UnsupportedOperationException(
							"Invalid metadata: " + e.getValue() + " for the connector: " + IDENTIFIER);
				}
				metadataConverters.put(e.getKey(), metadataConverter);
			}
			metadataConverterses.put(entry.getKey(), metadataConverters);
		}
		return metadataConverterses;
	}

}
