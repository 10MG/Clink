package cn.tenmg.clink.cdc.oracle.debezium;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DeserializationRuntimeConverter;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.utils.TemporalConversions;

import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.StringUtils;
import io.debezium.data.Envelope;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTime;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;

/**
 * 多表的从 Debezium 对象到 {@code Flink Table & SQL} 内部数据结构的反序列化模式
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.1
 */
public class MultiTableDebeziumDeserializationSchema implements DebeziumDeserializationSchema<Tuple2<String, Row>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1296320474773132864L;

	private final Map<String, RowType> rowTypes;

	private final Map<String, Map<Integer, MetadataConverter>> metadataConverters;

	private final boolean convertDeleteToUpdate;

	private final Map<String, DeserializationRuntimeConverter> physicalConverters = MapUtils.newHashMap();

	public MultiTableDebeziumDeserializationSchema(Map<String, RowType> rowTypes,
			Map<String, Map<Integer, MetadataConverter>> metadataConverters, boolean convertDeleteToUpdate) {
		this.rowTypes = rowTypes;
		this.metadataConverters = metadataConverters == null ? MapUtils.newHashMap() : metadataConverters;
		this.convertDeleteToUpdate = convertDeleteToUpdate;
		for (String tablename : this.rowTypes.keySet()) {
			RowType rowType = this.rowTypes.get(tablename);
			DeserializationRuntimeConverter physicalConverter = createNotNullConverter(rowType);
			this.physicalConverters.put(tablename, physicalConverter);
		}
	}

	@Override
	public void deserialize(SourceRecord record, Collector<Tuple2<String, Row>> out) throws Exception {
		Envelope.Operation op = Envelope.operationFor(record);
		Struct value = (Struct) record.value();
		Schema valueSchema = record.valueSchema();
		Struct source = value.getStruct("source");
		String schema = source.getString("schema"), tablename = source.get("table").toString();
		DeserializationRuntimeConverter physicalConverter = null;
		if (schema != null) {
			String fullName = StringUtils.concat(schema, ".", tablename);
			physicalConverter = physicalConverters.get(fullName);// 优先根据全名获取物理转换器
			if (physicalConverter == null) {
				physicalConverter = physicalConverters.get(tablename);
			} else {
				tablename = fullName;
			}
		} else {
			physicalConverter = physicalConverters.get(tablename);
		}

		if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
			Row insert = extractAfterRow(value, valueSchema, physicalConverter);
			insert.setKind(RowKind.INSERT);
			setMetadatas(tablename, insert, record);
			out.collect(Tuple2.of(tablename, insert));
		} else if (op == Envelope.Operation.DELETE) {
			if (convertDeleteToUpdate) {
				Row before = extractBeforeRow(value, valueSchema, physicalConverter);
				before.setKind(RowKind.UPDATE_BEFORE);
				setMetadatas(tablename, before, record);
				out.collect(Tuple2.of(tablename, before));

				int arity = before.getArity();
				Row after = new Row(RowKind.UPDATE_AFTER, arity);
				for (int i = 0; i < arity; i++) {
					after.setField(i, before.getField(i));
				}
				out.collect(Tuple2.of(tablename, after));
			} else {
				Row delete = extractBeforeRow(value, valueSchema, physicalConverter);
				delete.setKind(RowKind.DELETE);
				setMetadatas(tablename, delete, record);
				out.collect(Tuple2.of(tablename, delete));
			}
		} else {
			Row before = extractBeforeRow(value, valueSchema, physicalConverter);
			before.setKind(RowKind.UPDATE_BEFORE);
			setMetadatas(tablename, before, record);
			out.collect(Tuple2.of(tablename, before));

			Row after = extractAfterRow(value, valueSchema, physicalConverter);
			after.setKind(RowKind.UPDATE_AFTER);
			setMetadatas(tablename, after, record);
			out.collect(Tuple2.of(tablename, after));
		}
	}

	private Row extractAfterRow(Struct value, Schema valueSchema, DeserializationRuntimeConverter physicalConverter)
			throws Exception {
		Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
		Struct after = value.getStruct(Envelope.FieldName.AFTER);
		return (Row) physicalConverter.convert(after, afterSchema);
	}

	private Row extractBeforeRow(Struct value, Schema valueSchema, DeserializationRuntimeConverter physicalConverter)
			throws Exception {
		Schema beforeSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
		Struct before = value.getStruct(Envelope.FieldName.BEFORE);
		return (Row) physicalConverter.convert(before, beforeSchema);
	}

	private void setMetadatas(String tableName, Row row, SourceRecord record) {
		Entry<Integer, MetadataConverter> entry;
		for (Iterator<Entry<Integer, MetadataConverter>> it = metadataConverters.get(tableName).entrySet()
				.iterator(); it.hasNext();) {
			entry = it.next();
			row.setField(entry.getKey(), entry.getValue().read(record));
		}
	}

	@Override
	public TypeInformation<Tuple2<String, Row>> getProducedType() {
		return TypeInformation.of(new TypeHint<Tuple2<String, Row>>() {
		});
	}

	public static DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {

		switch (type.getTypeRoot()) {
		case NULL:
			return new DeserializationRuntimeConverter() {

				private static final long serialVersionUID = -5531578281942128706L;

				@Override
				public Object convert(Object dbzObj, Schema schema) {
					return null;
				}
			};
		case BOOLEAN:
			return convertToBoolean();
		case TINYINT:
			return new DeserializationRuntimeConverter() {

				private static final long serialVersionUID = 5826210245541496242L;

				@Override
				public Object convert(Object dbzObj, Schema schema) {
					return Byte.parseByte(dbzObj.toString());
				}
			};
		case SMALLINT:
			return new DeserializationRuntimeConverter() {

				private static final long serialVersionUID = 202642317319921270L;

				@Override
				public Object convert(Object dbzObj, Schema schema) {
					return Short.parseShort(dbzObj.toString());
				}
			};
		case INTEGER:
		case INTERVAL_YEAR_MONTH:
			return convertToInt();
		case BIGINT:
		case INTERVAL_DAY_TIME:
			return convertToLong();
		case DATE:
			return convertToDate();
		case TIME_WITHOUT_TIME_ZONE:
			return convertToTime();
		case TIMESTAMP_WITHOUT_TIME_ZONE:
			return convertToTimestamp(ZoneId.of("UTC"));
		case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
			return convertToLocalTimeZoneTimestamp(ZoneId.of("UTC"));
		case FLOAT:
			return convertToFloat();
		case DOUBLE:
			return convertToDouble();
		case CHAR:
		case VARCHAR:
			return convertToString();
		case BINARY:
		case VARBINARY:
			return convertToBinary();
		case DECIMAL:
			return createDecimalConverter((DecimalType) type);
		case ROW:
			return createRowConverter((RowType) type);
		case ARRAY:
		case MAP:
		case MULTISET:
		case RAW:
		default:
			throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	private static DeserializationRuntimeConverter convertToBoolean() {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = -4161081671092573975L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				if (dbzObj instanceof Boolean) {
					return dbzObj;
				} else if (dbzObj instanceof Byte) {
					return (byte) dbzObj == 1;
				} else if (dbzObj instanceof Short) {
					return (short) dbzObj == 1;
				} else {
					return Boolean.parseBoolean(dbzObj.toString());
				}
			}
		};
	}

	private static DeserializationRuntimeConverter convertToInt() {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = -7463894054578628927L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				if (dbzObj instanceof Integer) {
					return dbzObj;
				} else if (dbzObj instanceof Long) {
					return ((Long) dbzObj).intValue();
				} else {
					return Integer.parseInt(dbzObj.toString());
				}
			}
		};
	}

	private static DeserializationRuntimeConverter convertToLong() {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = 2855410652461690993L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				if (dbzObj instanceof Integer) {
					return ((Integer) dbzObj).longValue();
				} else if (dbzObj instanceof Long) {
					return dbzObj;
				} else {
					return Long.parseLong(dbzObj.toString());
				}
			}
		};
	}

	private static DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = -7856160950422977488L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				BigDecimal bigDecimal;
				if (dbzObj instanceof byte[]) {
					// decimal.handling.mode=precise
					bigDecimal = Decimal.toLogical(schema, (byte[]) dbzObj);
				} else if (dbzObj instanceof String) {
					// decimal.handling.mode=string
					bigDecimal = new BigDecimal((String) dbzObj);
				} else if (dbzObj instanceof Double) {
					// decimal.handling.mode=double
					bigDecimal = BigDecimal.valueOf((Double) dbzObj);
				} else {
					if (VariableScaleDecimal.LOGICAL_NAME.equals(schema.name())) {
						SpecialValueDecimal decimal = VariableScaleDecimal.toLogical((Struct) dbzObj);
						bigDecimal = decimal.getDecimalValue().orElse(BigDecimal.ZERO);
					} else {
						// fallback to string
						bigDecimal = new BigDecimal(dbzObj.toString());
					}
				}
				return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
			}
		};
	}

	private static DeserializationRuntimeConverter convertToDouble() {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = 2217554325155415123L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				if (dbzObj instanceof Float) {
					return ((Float) dbzObj).doubleValue();
				} else if (dbzObj instanceof Double) {
					return dbzObj;
				} else {
					return Double.parseDouble(dbzObj.toString());
				}
			}
		};
	}

	private static DeserializationRuntimeConverter convertToFloat() {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = 6586191898979457725L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				if (dbzObj instanceof Float) {
					return dbzObj;
				} else if (dbzObj instanceof Double) {
					return ((Double) dbzObj).floatValue();
				} else {
					return Float.parseFloat(dbzObj.toString());
				}
			}
		};
	}

	private static DeserializationRuntimeConverter convertToDate() {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = -4898068849755140790L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				return (int) TemporalConversions.toLocalDate(dbzObj).toEpochDay();
			}
		};
	}

	private static DeserializationRuntimeConverter convertToTime() {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = 6637768283717546945L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				if (dbzObj instanceof Long) {
					switch (schema.name()) {
					case MicroTime.SCHEMA_NAME:
						return (int) ((long) dbzObj / 1000);
					case NanoTime.SCHEMA_NAME:
						return (int) ((long) dbzObj / 1000_000);
					}
				} else if (dbzObj instanceof Integer) {
					return dbzObj;
				}
				// get number of milliseconds of the day
				return TemporalConversions.toLocalTime(dbzObj).toSecondOfDay() * 1000;
			}
		};
	}

	private static DeserializationRuntimeConverter convertToTimestamp(ZoneId serverTimeZone) {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = -3377296602526652049L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				if (dbzObj instanceof Long) {
					switch (schema.name()) {
					case Timestamp.SCHEMA_NAME:
						return TimestampData.fromEpochMillis((Long) dbzObj);
					case MicroTimestamp.SCHEMA_NAME:
						long micro = (long) dbzObj;
						return TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000));
					case NanoTimestamp.SCHEMA_NAME:
						long nano = (long) dbzObj;
						return TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000));
					}
				}
				LocalDateTime localDateTime = TemporalConversions.toLocalDateTime(dbzObj, serverTimeZone);
				return TimestampData.fromLocalDateTime(localDateTime);
			}
		};
	}

	private static DeserializationRuntimeConverter convertToLocalTimeZoneTimestamp(ZoneId serverTimeZone) {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = -504319898823637587L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				if (dbzObj instanceof String) {
					String str = (String) dbzObj;
					// TIMESTAMP_LTZ type is encoded in string type
					Instant instant = Instant.parse(str);
					return TimestampData.fromLocalDateTime(LocalDateTime.ofInstant(instant, serverTimeZone));
				}
				throw new IllegalArgumentException("Unable to convert to TimestampData from unexpected value '" + dbzObj
						+ "' of type " + dbzObj.getClass().getName());
			}
		};
	}

	private static DeserializationRuntimeConverter convertToString() {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = -5832036125040167926L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				return StringData.fromString(dbzObj.toString());
			}
		};
	}

	private static DeserializationRuntimeConverter convertToBinary() {
		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = 2035408470878061374L;

			@Override
			public Object convert(Object dbzObj, Schema schema) {
				if (dbzObj instanceof byte[]) {
					return dbzObj;
				} else if (dbzObj instanceof ByteBuffer) {
					ByteBuffer byteBuffer = (ByteBuffer) dbzObj;
					byte[] bytes = new byte[byteBuffer.remaining()];
					byteBuffer.get(bytes);
					return bytes;
				} else {
					throw new UnsupportedOperationException(
							"Unsupported BYTES value type: " + dbzObj.getClass().getSimpleName());
				}
			}
		};
	}

	private static DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		List<RowField> fields = rowType.getFields();
		int size = fields.size();
		final DeserializationRuntimeConverter[] fieldConverters = new DeserializationRuntimeConverter[size];
		for (int i = 0; i < size; i++) {
			fieldConverters[i] = createNotNullConverter(fields.get(i).getType());
		}
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[size]);

		return new DeserializationRuntimeConverter() {

			private static final long serialVersionUID = 5939600101571940500L;

			@Override
			public Object convert(Object dbzObj, Schema schema) throws Exception {
				Struct struct = (Struct) dbzObj;
				int arity = fieldNames.length;
				Row row = new Row(arity);
				for (int i = 0; i < arity; i++) {
					String fieldName = fieldNames[i];
					Field field = schema.field(fieldName);
					if (field == null) {
						fieldName = fieldName.toUpperCase();
						field = schema.field(fieldName);
						if (field == null) {
							fieldName = fieldName.toLowerCase();
							field = schema.field(fieldName);
						}
					}
					if (field == null) {
						row.setField(i, null);
					} else {
						Object fieldValue = struct.getWithoutDefault(fieldName);
						Schema fieldSchema = field.schema();
						Object convertedField = convertField(fieldConverters[i], fieldValue, fieldSchema);
						row.setField(i, convertedField);
					}
				}
				return row;
			}
		};
	}

	private static Object convertField(DeserializationRuntimeConverter fieldConverter, Object fieldValue,
			Schema fieldSchema) throws Exception {
		if (fieldValue == null) {
			return null;
		} else {
			return fieldConverter.convert(fieldValue, fieldSchema);
		}
	}
}
