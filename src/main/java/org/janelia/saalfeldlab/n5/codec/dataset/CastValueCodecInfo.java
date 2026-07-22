package org.janelia.saalfeldlab.n5.codec.dataset;

import java.util.Objects;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import com.google.gson.annotations.SerializedName;

/**
 * Describes an "array -&gt; array" transformation that converts numerical values
 * to a different data type, without reinterpreting binary representations.
 * <p>
 * The required {@code data_type} parameter gives the type values are cast to
 * when encoding. The optional {@code rounding} parameter selects how
 * non-integral values are rounded (default {@link Rounding#NEAREST_EVEN}), and
 * the optional {@code out_of_range} parameter selects how values outside the
 * target type's range are handled; if it is absent, such values raise an error.
 * <p>
 * The {@code scalar_map} parameter of the specification is not yet supported.
 * <p>
 * See the specification of
 * <a href="https://github.com/zarr-developers/zarr-extensions/blob/main/codecs/cast_value/README.md">Zarr's cast_value codec</a>.
 */
@NameConfig.Name(value = CastValueCodecInfo.TYPE)
public class CastValueCodecInfo implements DatasetCodecInfo {

	private static final long serialVersionUID = -4474779692197638242L;

	public static final String TYPE = "cast_value";

	public static final Rounding DEFAULT_ROUNDING = Rounding.NEAREST_EVEN;

	/**
	 * How non-integral values are rounded when cast to an integral type.
	 */
	public enum Rounding {

		/** IEEE 754 {@code roundTiesToEven}. */
		@SerializedName("nearest-even")
		NEAREST_EVEN("nearest-even"),

		/** Truncate towards zero. */
		@SerializedName("towards-zero")
		TOWARDS_ZERO("towards-zero"),

		/** Round towards positive infinity (ceiling). */
		@SerializedName("towards-positive")
		TOWARDS_POSITIVE("towards-positive"),

		/** Round towards negative infinity (floor). */
		@SerializedName("towards-negative")
		TOWARDS_NEGATIVE("towards-negative"),

		/** Round to nearest, with ties away from zero. */
		@SerializedName("nearest-away")
		NEAREST_AWAY("nearest-away");

		private final String label;

		Rounding(final String label) {

			this.label = label;
		}

		@Override
		public String toString() {

			return label;
		}

		public static Rounding fromString(final String string) {

			for (final Rounding value : values())
				if (value.label.equals(string))
					return value;

			return null;
		}
	}

	/**
	 * How values outside the range of the target data type are handled. If no
	 * policy is configured, out-of-range values raise an error.
	 */
	public enum OutOfRange {

		/**
		 * Map to the smallest/largest representable value, using
		 * &plusmn;Infinity for types that support it.
		 */
		@SerializedName("clamp")
		CLAMP("clamp"),

		/** Modular arithmetic; integral types only. */
		@SerializedName("wrap")
		WRAP("wrap");

		private final String label;

		OutOfRange(final String label) {

			this.label = label;
		}

		@Override
		public String toString() {

			return label;
		}

		public static OutOfRange fromString(final String string) {

			for (final OutOfRange value : values())
				if (value.label.equals(string))
					return value;

			return null;
		}
	}

	@NameConfig.Parameter(value = "data_type")
	private DataType dataType;

	@NameConfig.Parameter(optional = true)
	private Rounding rounding;

	@NameConfig.Parameter(value = "out_of_range", optional = true)
	private OutOfRange outOfRange;

	public CastValueCodecInfo() {
		// no-arg constructor for serialization
	}

	public CastValueCodecInfo(final DataType dataType) {

		this(dataType, DEFAULT_ROUNDING, null);
	}

	public CastValueCodecInfo(final DataType dataType, final Rounding rounding, final OutOfRange outOfRange) {

		this.dataType = dataType;
		this.rounding = rounding;
		this.outOfRange = outOfRange;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	/**
	 * @return the data type values are cast to when encoding
	 */
	public DataType getDataType() {

		return dataType;
	}

	/**
	 * @return the configured rounding mode, or {@link #DEFAULT_ROUNDING} if none
	 *         was given
	 */
	public Rounding getRounding() {

		return rounding == null ? DEFAULT_ROUNDING : rounding;
	}

	/**
	 * @return the configured out-of-range policy, or {@code null} if
	 *         out-of-range values should raise an error
	 */
	public OutOfRange getOutOfRange() {

		return outOfRange;
	}

	@Override
	public CastValueCodec<?, ?> create(final DatasetAttributes attributes) {

		validate(attributes.getDataType());
		return new CastValueCodec<Object, Object>(attributes.getDataType(), dataType, getRounding(), outOfRange);
	}

	private void validate(final DataType sourceDataType) {

		if (dataType == null)
			throw new N5Exception("CastValueCodec requires a data_type");

		if (outOfRange == OutOfRange.WRAP && BlockElementAccess.isFloatingPoint(dataType))
			throw new N5Exception("CastValueCodec out_of_range \"wrap\" is only valid for integral data types, but "
					+ "data_type is " + dataType);

		// The codec selects its per-type Casters up front, so a non-numerical
		// type would fail at construction rather than on the first element.
		// Report it here instead, where the message can name which side is wrong.
		requireNumerical(sourceDataType, "the dataset's data type");
		requireNumerical(dataType, "data_type");
	}

	private static void requireNumerical(final DataType type, final String what) {

		if (type == DataType.STRING || type == DataType.OBJECT)
			throw new N5Exception("CastValueCodec cannot convert " + what + " " + type
					+ "; only numerical data types are supported");
	}

	@Override
	public boolean equals(final Object obj) {

		if (obj instanceof CastValueCodecInfo) {
			final CastValueCodecInfo other = (CastValueCodecInfo)obj;
			return dataType == other.dataType
					&& getRounding() == other.getRounding()
					&& outOfRange == other.outOfRange;
		}
		return false;
	}

	@Override
	public int hashCode() {

		return Objects.hash(dataType, getRounding(), outOfRange);
	}

}
