package org.janelia.saalfeldlab.n5.codec.dataset;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.DatasetCodec;
import org.janelia.saalfeldlab.n5.codec.dataset.BlockElementAccess.DoubleReader;
import org.janelia.saalfeldlab.n5.codec.dataset.BlockElementAccess.DoubleWriter;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.OutOfRange;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.Rounding;

/**
 * An "array -&gt; array" codec that converts numerical values to a different
 * data type, without reinterpreting binary representations.
 * <p>
 * Encoding casts from the dataset's data type to the codec's target
 * {@code data_type}; decoding casts back. Values are converted element-wise via
 * {@code double} arithmetic, applying, in order:
 * <ol>
 * <li>an exact representability check,</li>
 * <li>the configured {@link Rounding} mode, and</li>
 * <li>the configured {@link OutOfRange} policy.</li>
 * </ol>
 * A value that is not representable in the target type and for which no
 * {@code out_of_range} policy is configured raises an {@link N5IOException}.
 * <p>
 * NaN and infinities propagate between floating-point types, and signed zero is
 * preserved. The {@code scalar_map} parameter of the specification is not yet
 * supported.
 *
 * @param <S>
 *            the source (decoded) block data type
 * @param <T>
 *            the target (encoded) block data type
 *
 * @see CastValueCodecInfo
 */
public class CastValueCodec<S, T> implements DatasetCodec<S, T> {

	private final DataType sourceDataType;

	private final DataType targetDataType;

	private final Rounding rounding;

	private final OutOfRange outOfRange;

	public CastValueCodec(
			final DataType sourceDataType,
			final DataType targetDataType,
			final Rounding rounding,
			final OutOfRange outOfRange) {

		this.sourceDataType = sourceDataType;
		this.targetDataType = targetDataType;
		this.rounding = rounding == null ? CastValueCodecInfo.DEFAULT_ROUNDING : rounding;
		this.outOfRange = outOfRange;
	}

	public DataType getSourceDataType() {

		return sourceDataType;
	}

	public DataType getTargetDataType() {

		return targetDataType;
	}

	@Override
	public DataBlock<T> encode(final DataBlock<S> block) throws N5IOException {

		return cast(block, sourceDataType, targetDataType);
	}

	@Override
	public DataBlock<S> decode(final DataBlock<T> block) throws N5IOException {

		return cast(block, targetDataType, sourceDataType);
	}

	@SuppressWarnings("unchecked")
	private <X, Y> DataBlock<Y> cast(final DataBlock<X> in, final DataType from, final DataType to)
			throws N5IOException {

		final DataBlock<Y> out = (DataBlock<Y>)to.createDataBlock(
				in.getSize(), in.getGridPosition(), in.getNumElements());

		final DoubleReader reader = BlockElementAccess.reader(from);
		final DoubleWriter writer = BlockElementAccess.writer(to);

		final Object src = in.getData();
		final Object dst = out.getData();

		final int n = in.getNumElements();
		for (int i = 0; i < n; i++)
			writer.set(dst, i, convert(reader.get(src, i), to));

		return out;
	}

	/**
	 * Converts a single value to one representable in the {@code target} type.
	 */
	private double convert(final double value, final DataType target) throws N5IOException {

		if (BlockElementAccess.isFloatingPoint(target))
			return convertToFloatingPoint(value, target);

		return convertToIntegral(value, target);
	}

	private double convertToFloatingPoint(final double value, final DataType target) throws N5IOException {

		// NaN and infinities propagate; float64 represents every double exactly
		if (target == DataType.FLOAT64 || !Double.isFinite(value))
			return value;

		// float32: a finite double may still overflow the float range
		final float asFloat = (float)value;
		if (!Float.isInfinite(asFloat))
			return asFloat;

		if (outOfRange == OutOfRange.CLAMP)
			return Math.copySign(Double.POSITIVE_INFINITY, value);

		throw outOfRangeException(value, target);
	}

	private double convertToIntegral(final double value, final DataType target) throws N5IOException {

		if (Double.isNaN(value))
			throw new N5IOException("NaN is not representable as " + target
					+ "; a scalar_map entry would be required to encode it");

		if (Double.isInfinite(value)) {
			if (outOfRange == OutOfRange.CLAMP)
				return value > 0 ? BlockElementAccess.maxValue(target) : BlockElementAccess.minValue(target);

			// wrapping an infinity is not meaningful
			throw outOfRangeException(value, target);
		}

		final double rounded = round(value);
		if (rounded >= BlockElementAccess.minValue(target) && rounded <= BlockElementAccess.maxValue(target))
			return rounded;

		if (outOfRange == null)
			throw outOfRangeException(value, target);

		switch (outOfRange) {
		case CLAMP:
			return Math.min(BlockElementAccess.maxValue(target),
					Math.max(BlockElementAccess.minValue(target), rounded));
		case WRAP:
			return wrap(rounded, target);
		default:
			throw outOfRangeException(value, target);
		}
	}

	/**
	 * Rounds {@code value} to an integral value according to the configured
	 * {@link Rounding} mode.
	 */
	private double round(final double value) {

		switch (rounding) {
		case NEAREST_EVEN:
			return Math.rint(value);
		case TOWARDS_ZERO:
			return value < 0 ? Math.ceil(value) : Math.floor(value);
		case TOWARDS_POSITIVE:
			return Math.ceil(value);
		case TOWARDS_NEGATIVE:
			return Math.floor(value);
		case NEAREST_AWAY: {
			final double magnitude = Math.abs(value);
			double result = Math.floor(magnitude + 0.5);
			// guard the case where magnitude + 0.5 rounds up on its own
			if (result - magnitude > 0.5)
				result -= 1.0;
			return Math.copySign(result, value);
		}
		default:
			throw new N5IOException("Unsupported rounding mode " + rounding);
		}
	}

	/**
	 * Reduces {@code value} modulo 2<sup>bits</sup> into
	 * {@code [0, 2}<sup>{@code bits}</sup>{@code )}. Truncating the result to the
	 * width of {@code target} yields the correct two's-complement representation
	 * for signed types as well.
	 */
	private static double wrap(final double value, final DataType target) {

		final double modulus = Math.scalb(1.0, BlockElementAccess.bits(target));
		double wrapped = value - Math.floor(value / modulus) * modulus;

		// guard against a floating-point result landing outside [0, modulus)
		if (wrapped >= modulus)
			wrapped -= modulus;
		if (wrapped < 0)
			wrapped += modulus;

		return wrapped;
	}

	private static N5IOException outOfRangeException(final double value, final DataType target) {

		return new N5IOException(value + " is not representable as " + target
				+ "; set out_of_range to \"clamp\" or \"wrap\" to permit this");
	}
}
