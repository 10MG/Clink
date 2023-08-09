package cn.tenmg.clink.data.type.logical;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import cn.tenmg.clink.data.type.MetaDataType;

@PublicEvolving
public final class LocalZonedTimestampType extends LogicalType implements MetaDataType {
	private static final long serialVersionUID = 1L;

	public static final int MIN_PRECISION = TimestampType.MIN_PRECISION;

	public static final int MAX_PRECISION = TimestampType.MAX_PRECISION;

	public static final int DEFAULT_PRECISION = TimestampType.DEFAULT_PRECISION;

	private static final String FORMAT = "TIMESTAMP(%d) WITH LOCAL TIME ZONE";

	private static final String SUMMARY_FORMAT = "TIMESTAMP_LTZ(%d)";

	private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(java.time.Instant.class.getName(),
			Integer.class.getName(), Long.class.getName(), TimestampData.class.getName(),
			java.sql.Timestamp.class.getName());

	private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(java.time.Instant.class.getName(),
			Integer.class.getName(), int.class.getName(), Long.class.getName(), long.class.getName(),
			TimestampData.class.getName(), java.sql.Timestamp.class.getName());

	private static final Class<?> DEFAULT_CONVERSION = java.time.Instant.class;

	private final String key;

	private final TimestampKind kind;

	private final int precision;

	/**
	 * Internal constructor that allows attaching additional metadata about time
	 * attribute properties. The additional metadata does not affect equality or
	 * serializability.
	 *
	 * <p>
	 * Use {@link #getKind()} for comparing this metadata.
	 */
	@Internal
	public LocalZonedTimestampType(String key, boolean isNullable, TimestampKind kind, int precision) {
		super(isNullable, LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
		if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
			throw new ValidationException(String.format(
					"Timestamp with local time zone precision must be between %d and %d (both inclusive).",
					MIN_PRECISION, MAX_PRECISION));
		}
		this.key = key;
		this.kind = kind;
		this.precision = precision;
	}

	public LocalZonedTimestampType(String key, boolean isNullable, int precision) {
		this(key, isNullable, TimestampKind.REGULAR, precision);
	}

	public LocalZonedTimestampType(String key, int precision) {
		this(key, true, precision);
	}

	public LocalZonedTimestampType(String key) {
		this(key, DEFAULT_PRECISION);
	}

	@Override
	public String getKey() {
		return key;
	}

	@Internal
	public TimestampKind getKind() {
		return kind;
	}

	public int getPrecision() {
		return precision;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new LocalZonedTimestampType(key, isNullable, kind, precision);
	}

	@Override
	public String asSerializableString() {
		return withNullability(FORMAT, precision);
	}

	@Override
	public String asSummaryString() {
		if (kind != TimestampKind.REGULAR) {
			return String.format("%s *%s*", withNullability(SUMMARY_FORMAT, precision), kind);
		}
		return withNullability(SUMMARY_FORMAT, precision);
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		if (isNullable()) {
			return NULL_OUTPUT_CONVERSION.contains(clazz.getName());
		}
		return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public Class<?> getDefaultConversion() {
		return DEFAULT_CONVERSION;
	}

	@Override
	public List<LogicalType> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(LogicalTypeVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		LocalZonedTimestampType that = (LocalZonedTimestampType) o;
		return precision == that.precision;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), precision);
	}
}
