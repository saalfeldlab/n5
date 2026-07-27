package org.janelia.saalfeldlab.n5.codec.dataset;

import java.util.Objects;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.Rounding;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * Describes an "array -&gt; array" transformation that fuses a {@code scale_offset},
 * a saturating clamp to an arbitrary band, and a narrowing cast into one pass.
 * <p>
 * Encoding maps each value {@code v} through
 * <pre>    out = round(clamp((v - offset) * scale, clamp_min, clamp_max))</pre>
 * and stores it as {@code data_type}; decoding inverts the affine part only,
 * {@code (in / scale) + offset}.
 * <p>
 * {@code scale} and {@code offset} are optional and default to {@code 1} and
 * {@code 0}. {@code clamp_min} and {@code clamp_max} are optional and default to
 * the target type's full representable range &mdash; making this codec a strict
 * superset of {@code scale_offset} followed by {@code cast_value} with
 * {@code out_of_range = clamp}. Their purpose is to clamp to a band strictly
 * <em>inside</em> the target type's range, which the standard codecs cannot
 * express directly. The {@code rounding} parameter matches {@link CastValueCodecInfo}
 * (default {@link Rounding#NEAREST_EVEN}).
 *
 * @see ScaleOffsetCastCodec
 */
@NameConfig.Name(value = ScaleOffsetCastCodecInfo.TYPE)
public class ScaleOffsetCastCodecInfo implements DatasetCodecInfo {

	private static final long serialVersionUID = 6931061954592005657L;

	public static final String TYPE = "scale_offset_cast";

	public static final double DEFAULT_SCALE = 1;

	public static final double DEFAULT_OFFSET = 0;

	public static final Rounding DEFAULT_ROUNDING = CastValueCodecInfo.DEFAULT_ROUNDING;

	@NameConfig.Parameter(optional = true)
	private double scale;

	@NameConfig.Parameter(optional = true)
	private double offset;

	@NameConfig.Parameter(value = "data_type")
	private DataType dataType;

	@NameConfig.Parameter(value = "clamp_min", optional = true)
	private Double clampMin;

	@NameConfig.Parameter(value = "clamp_max", optional = true)
	private Double clampMax;

	@NameConfig.Parameter(optional = true)
	private Rounding rounding;

	public ScaleOffsetCastCodecInfo() {

		// no-arg constructor for serialization; establishes the optional defaults
		this.scale = DEFAULT_SCALE;
		this.offset = DEFAULT_OFFSET;
	}

	public ScaleOffsetCastCodecInfo(
			final DataType dataType,
			final double scale,
			final double offset,
			final Double clampMin,
			final Double clampMax,
			final Rounding rounding) {

		this.dataType = dataType;
		this.scale = scale;
		this.offset = offset;
		this.clampMin = clampMin;
		this.clampMax = clampMax;
		this.rounding = rounding;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public DataType getDataType() {

		return dataType;
	}

	public double getScale() {

		return scale;
	}

	public double getOffset() {

		return offset;
	}

	/**
	 * @return the configured lower clamp bound, or {@code null} to default to the
	 *         target type's minimum
	 */
	public Double getClampMin() {

		return clampMin;
	}

	/**
	 * @return the configured upper clamp bound, or {@code null} to default to the
	 *         target type's maximum
	 */
	public Double getClampMax() {

		return clampMax;
	}

	public Rounding getRounding() {

		return rounding == null ? DEFAULT_ROUNDING : rounding;
	}

	@Override
	public ScaleOffsetCastCodec<?, ?> create(final DatasetAttributes attributes) {

		return create(attributes, attributes.getDataType());
	}

	@Override
	public ScaleOffsetCastCodec<?, ?> create(final DatasetAttributes attributes, final DataType sourceDataType) {

		validate(sourceDataType);

		final double min = clampMin != null ? clampMin : defaultMin(dataType);
		final double max = clampMax != null ? clampMax : defaultMax(dataType);

		return new ScaleOffsetCastCodec<Object, Object>(
				sourceDataType, dataType, scale, offset, min, max, getRounding());
	}

	@Override
	public DataType encodedDataType(final DataType dataType) {

		// converting the data type is part of the point of this codec
		return this.dataType;
	}

	private void validate(final DataType sourceDataType) {

		if (dataType == null)
			throw new N5Exception("ScaleOffsetCastCodec requires a data_type");

		requireNumerical(sourceDataType, "the dataset's data type");
		requireNumerical(dataType, "data_type");

		if (clampMin != null && clampMax != null && clampMin > clampMax)
			throw new N5Exception("ScaleOffsetCastCodec clamp_min (" + clampMin
					+ ") must not exceed clamp_max (" + clampMax + ")");

		// A band outside the target type's range would overflow on narrowing;
		// the clamp only makes sense inside it. Only checkable for integral
		// targets, whose range is finite.
		if (!BlockElementAccess.isFloatingPoint(dataType)) {
			final double typeMin = BlockElementAccess.minValue(dataType);
			final double typeMax = BlockElementAccess.maxValue(dataType);
			if (clampMin != null && clampMin < typeMin)
				throw new N5Exception("ScaleOffsetCastCodec clamp_min (" + clampMin
						+ ") is below the range of " + dataType + " [" + typeMin + ", " + typeMax + "]");
			if (clampMax != null && clampMax > typeMax)
				throw new N5Exception("ScaleOffsetCastCodec clamp_max (" + clampMax
						+ ") is above the range of " + dataType + " [" + typeMin + ", " + typeMax + "]");
		}
	}

	private static double defaultMin(final DataType type) {

		return BlockElementAccess.isFloatingPoint(type)
				? Double.NEGATIVE_INFINITY
				: BlockElementAccess.minValue(type);
	}

	private static double defaultMax(final DataType type) {

		return BlockElementAccess.isFloatingPoint(type)
				? Double.POSITIVE_INFINITY
				: BlockElementAccess.maxValue(type);
	}

	private static void requireNumerical(final DataType type, final String what) {

		if (type == DataType.STRING || type == DataType.OBJECT)
			throw new N5Exception("ScaleOffsetCastCodec cannot convert " + what + " " + type
					+ "; only numerical data types are supported");
	}

	@Override
	public boolean equals(final Object obj) {

		if (obj instanceof ScaleOffsetCastCodecInfo) {
			final ScaleOffsetCastCodecInfo other = (ScaleOffsetCastCodecInfo)obj;
			return dataType == other.dataType
					&& scale == other.scale
					&& offset == other.offset
					&& Objects.equals(clampMin, other.clampMin)
					&& Objects.equals(clampMax, other.clampMax)
					&& getRounding() == other.getRounding();
		}
		return false;
	}

	@Override
	public int hashCode() {

		return Objects.hash(dataType, scale, offset, clampMin, clampMax, getRounding());
	}
}
