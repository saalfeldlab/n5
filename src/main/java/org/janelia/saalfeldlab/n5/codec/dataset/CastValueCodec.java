package org.janelia.saalfeldlab.n5.codec.dataset;

import java.util.function.DoubleUnaryOperator;

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
 * <p>
 * <b>Structure.</b> The conversion lives in {@link Caster}, which has one
 * concrete implementation per output {@link DataType}, in the manner of
 * {@link org.janelia.saalfeldlab.n5.codec.FlatArrayCodec FlatArrayCodec}. A
 * {@code Caster} is selected once per direction at construction time, so the
 * output array type and the target's range bounds are compile-time constants
 * inside the element loop. Encoding and decoding are the same operation with
 * the types exchanged, so the codec holds two: one writing the target type, one
 * writing the source type.
 * <p>
 * <b>Why the loops look the way they do.</b> Measurement (see
 * {@code CastValueVectorizationProbe}) showed that per-element <em>branches</em>
 * dominate this codec's cost &mdash; not type dispatch, and not the
 * {@link DoubleReader} indirection, which is free. Testing each value for NaN
 * and for range accounted for roughly two thirds of the time spent above the
 * allocate-and-copy floor, because those branches keep the loop scalar. The
 * element loops here are therefore written branch-free, accumulating validity
 * flags with non-short-circuiting operators and inspecting them once after the
 * loop. See {@link Caster} for how each out-of-range policy is handled.
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

	/** Reads the source type; used when encoding. */
	private final DoubleReader sourceReader;

	/** Reads the target type; used when decoding. */
	private final DoubleReader targetReader;

	/** Writes the target type; used when encoding. */
	private final Caster encodeCaster;

	/** Writes the source type; used when decoding. */
	private final Caster decodeCaster;

	public CastValueCodec(
			final DataType sourceDataType,
			final DataType targetDataType,
			final Rounding rounding,
			final OutOfRange outOfRange) {

		this.sourceDataType = sourceDataType;
		this.targetDataType = targetDataType;
		this.rounding = rounding == null ? CastValueCodecInfo.DEFAULT_ROUNDING : rounding;
		this.outOfRange = outOfRange;

		this.sourceReader = BlockElementAccess.reader(sourceDataType);
		this.targetReader = BlockElementAccess.reader(targetDataType);
		this.encodeCaster = Caster.forDataType(targetDataType, this.rounding, outOfRange);
		this.decodeCaster = Caster.forDataType(sourceDataType, this.rounding, outOfRange);
	}

	public DataType getSourceDataType() {

		return sourceDataType;
	}

	public DataType getTargetDataType() {

		return targetDataType;
	}

	public Rounding getRounding() {

		return rounding;
	}

	public OutOfRange getOutOfRange() {

		return outOfRange;
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataBlock<T> encode(final DataBlock<S> block) throws N5IOException {

		return (DataBlock<T>)encodeCaster.cast(block, sourceReader);
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataBlock<S> decode(final DataBlock<T> block) throws N5IOException {

		return (DataBlock<S>)decodeCaster.cast(block, targetReader);
	}

	// ------------------------------------------------------------------
	// Casters: one per output DataType
	// ------------------------------------------------------------------

	/**
	 * Converts a whole {@link DataBlock} into a new block of one fixed output
	 * {@link DataType}, reading the input through a {@link DoubleReader}.
	 * <p>
	 * Subclasses supply the output array type and its range bounds as literals,
	 * so the bounds comparisons fold to constants once inlined.
	 * <p>
	 * <b>Out-of-range policies.</b> The default (error) policy and
	 * {@link OutOfRange#CLAMP} share a single branch-free loop: it always stores
	 * the clamped value and always accumulates an {@code outOfRange} flag.
	 * {@code CLAMP} then ignores the flag &mdash; the clamped value <em>is</em>
	 * the answer &mdash; while the error policy inspects it once at the end.
	 * NaN is accumulated separately, since a NaN reaching an integral target is
	 * an error under every policy.
	 * <p>
	 * When either flag trips, the block is re-converted through
	 * {@link #castChecked}, the original scalar implementation, purely so the
	 * exception reports the same offending value and message as before. That
	 * path ends in a throw, so its cost is irrelevant.
	 * <p>
	 * {@link OutOfRange#WRAP} needs genuinely different arithmetic and is not
	 * the hot case, so it delegates to {@link #castChecked} outright.
	 */
	abstract static class Caster {

		private final DataType targetType;

		private final Rounding rounding;

		private final OutOfRange outOfRange;

		/**
		 * The rounding mode as a function, resolved once. Selecting it here
		 * rather than switching on the enum per element keeps the switch out of
		 * the loop; the call site is monomorphic for a given codec instance and
		 * inlines, as it does in {@link ScaleOffsetCodec}.
		 */
		private final DoubleUnaryOperator rounder;

		Caster(final DataType targetType, final Rounding rounding, final OutOfRange outOfRange) {

			this.targetType = targetType;
			this.rounding = rounding;
			this.outOfRange = outOfRange;
			this.rounder = rounderFor(rounding);
		}

		/**
		 * Returns a {@code Caster} writing {@code dataType}.
		 *
		 * @throws N5IOException
		 *             if {@code dataType} is not numerical
		 */
		static Caster forDataType(final DataType dataType, final Rounding rounding, final OutOfRange outOfRange) {

			switch (dataType) {
			case INT8:
				return new ToInt8Caster(rounding, outOfRange);
			case UINT8:
				return new ToUint8Caster(rounding, outOfRange);
			case INT16:
				return new ToInt16Caster(rounding, outOfRange);
			case UINT16:
				return new ToUint16Caster(rounding, outOfRange);
			case INT32:
				return new ToInt32Caster(rounding, outOfRange);
			case UINT32:
				return new ToUint32Caster(rounding, outOfRange);
			case INT64:
				return new ToInt64Caster(rounding, outOfRange);
			case UINT64:
				return new ToUint64Caster(rounding, outOfRange);
			case FLOAT32:
				return new ToFloat32Caster(rounding, outOfRange);
			case FLOAT64:
				return new ToFloat64Caster(rounding, outOfRange);
			default:
				throw new N5IOException("CastValueCodec cannot cast to data type " + dataType);
			}
		}

		abstract DataBlock<?> cast(DataBlock<?> in, DoubleReader reader) throws N5IOException;

		final DataBlock<?> newBlock(final DataBlock<?> in) {

			return targetType.createDataBlock(in.getSize(), in.getGridPosition(), in.getNumElements());
		}

		/**
		 * @return whether the branch-free loop applies. {@link OutOfRange#WRAP}
		 *         needs modular arithmetic and takes {@link #castChecked}.
		 */
		final boolean branchFree() {

			return outOfRange != OutOfRange.WRAP;
		}

		/**
		 * @return whether accumulated flags require re-running the checked path
		 *         to raise an exception. NaN is always an error for integral
		 *         targets; an out-of-range value is an error only when no policy
		 *         is configured.
		 */
		final boolean mustReport(final boolean sawNaN, final boolean sawOutOfRange) {

			return sawNaN | (sawOutOfRange & (outOfRange == null));
		}

		/**
		 * @return whether a float-range overflow must be reported rather than
		 *         handled. NaN is not an error for a float target, and only
		 *         {@link OutOfRange#CLAMP} permits an overflow &mdash; {@code WRAP}
		 *         is meaningless here and raises, as it did before.
		 */
		final boolean mustReportOverflow(final boolean sawOverflow) {

			return sawOverflow & (outOfRange != OutOfRange.CLAMP);
		}

		final double round(final double value) {

			return rounder.applyAsDouble(value);
		}

		/**
		 * The original scalar implementation: one fully checked conversion per
		 * element, via {@link BlockElementAccess#writer}. Used for
		 * {@link OutOfRange#WRAP}, and to raise the exception when the
		 * branch-free loop has flagged a problem.
		 */
		final DataBlock<?> castChecked(
				final DataBlock<?> in, final DoubleReader reader,
				final double min, final double max, final int bits) throws N5IOException {

			final DataBlock<?> out = newBlock(in);
			final DoubleWriter writer = BlockElementAccess.writer(targetType);

			final Object src = in.getData();
			final Object dst = out.getData();

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++)
				writer.set(dst, i, toIntegral(reader.get(src, i), min, max, bits));

			return out;
		}

		/**
		 * Converts {@code value} to an integral value within {@code [min, max]},
		 * applying the configured rounding mode and out-of-range policy.
		 */
		final double toIntegral(final double value, final double min, final double max, final int bits)
				throws N5IOException {

			if (Double.isNaN(value))
				throw new N5IOException("NaN is not representable as " + targetType
						+ "; a scalar_map entry would be required to encode it");

			if (Double.isInfinite(value)) {
				if (outOfRange == OutOfRange.CLAMP)
					return value > 0 ? max : min;

				// wrapping an infinity is not meaningful
				throw outOfRangeException(value);
			}

			final double rounded = round(value);
			if (rounded >= min && rounded <= max)
				return rounded;

			if (outOfRange == null)
				throw outOfRangeException(value);

			switch (outOfRange) {
			case CLAMP:
				return Math.min(max, Math.max(min, rounded));
			case WRAP:
				return wrap(rounded, bits);
			default:
				throw outOfRangeException(value);
			}
		}

		/**
		 * Clamps a rounded value into {@code [min, max]}. NaN propagates, which
		 * the caller detects separately.
		 */
		static double clamp(final double rounded, final double min, final double max) {

			return Math.min(max, Math.max(min, rounded));
		}

		/**
		 * @return whether {@code rounded} lies outside {@code [min, max]},
		 *         computed without branching. A NaN input yields {@code false}
		 *         here and is caught by the separate NaN flag.
		 */
		static boolean outside(final double rounded, final double min, final double max) {

			return (rounded < min) | (rounded > max);
		}

		private static DoubleUnaryOperator rounderFor(final Rounding rounding) {

			switch (rounding) {
			case NEAREST_EVEN:
				return Math::rint;
			case TOWARDS_ZERO:
				return value -> value < 0 ? Math.ceil(value) : Math.floor(value);
			case TOWARDS_POSITIVE:
				return Math::ceil;
			case TOWARDS_NEGATIVE:
				return Math::floor;
			case NEAREST_AWAY:
				return Caster::roundNearestAway;
			default:
				throw new N5IOException("Unsupported rounding mode " + rounding);
			}
		}

		private static double roundNearestAway(final double value) {

			final double magnitude = Math.abs(value);
			double result = Math.floor(magnitude + 0.5);
			// guard the case where magnitude + 0.5 rounds up on its own
			if (result - magnitude > 0.5)
				result -= 1.0;
			return Math.copySign(result, value);
		}

		/**
		 * Reduces {@code value} modulo 2<sup>bits</sup> into
		 * {@code [0, 2}<sup>{@code bits}</sup>{@code )}. Truncating the result to
		 * the width of the target type yields the correct two's-complement
		 * representation for signed types as well.
		 */
		private static double wrap(final double value, final int bits) {

			final double modulus = Math.scalb(1.0, bits);
			double wrapped = value - Math.floor(value / modulus) * modulus;

			// guard against a floating-point result landing outside [0, modulus)
			if (wrapped >= modulus)
				wrapped -= modulus;
			if (wrapped < 0)
				wrapped += modulus;

			return wrapped;
		}

		final N5IOException outOfRangeException(final double value) {

			return new N5IOException(value + " is not representable as " + targetType
					+ "; set out_of_range to \"clamp\" or \"wrap\" to permit this");
		}
	}

	// ------------------------------------------------------------------
	// Integral targets
	//
	// Narrowing note: after clamping, a value bound for an 8-, 16- or signed
	// 32-bit target provably fits in an int, so these use (int) rather than
	// (long). Java's d2l carries saturation semantics that measured ~23% of
	// this codec's overhead. uint32 cannot: its clamped maximum, 2^32 - 1,
	// saturates d2i. The 64-bit targets need the full long.
	// ------------------------------------------------------------------

	private static final class ToInt8Caster extends Caster {

		private static final double MIN = Byte.MIN_VALUE;
		private static final double MAX = Byte.MAX_VALUE;
		private static final int BITS = 8;

		ToInt8Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.INT8, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			if (!branchFree())
				return castChecked(in, reader, MIN, MAX, BITS);

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final byte[] dst = (byte[])out.getData();

			boolean sawNaN = false;
			boolean sawOutOfRange = false;

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final double r = round(v);
				sawNaN |= v != v;
				sawOutOfRange |= outside(r, MIN, MAX);
				dst[i] = (byte)(int)clamp(r, MIN, MAX);
			}

			if (mustReport(sawNaN, sawOutOfRange))
				return castChecked(in, reader, MIN, MAX, BITS);

			return out;
		}
	}

	private static final class ToUint8Caster extends Caster {

		private static final double MIN = 0;
		private static final double MAX = 0xffL;
		private static final int BITS = 8;

		ToUint8Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.UINT8, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			if (!branchFree())
				return castChecked(in, reader, MIN, MAX, BITS);

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final byte[] dst = (byte[])out.getData();

			boolean sawNaN = false;
			boolean sawOutOfRange = false;

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final double r = round(v);
				sawNaN |= v != v;
				sawOutOfRange |= outside(r, MIN, MAX);
				dst[i] = (byte)(int)clamp(r, MIN, MAX);
			}

			if (mustReport(sawNaN, sawOutOfRange))
				return castChecked(in, reader, MIN, MAX, BITS);

			return out;
		}
	}

	private static final class ToInt16Caster extends Caster {

		private static final double MIN = Short.MIN_VALUE;
		private static final double MAX = Short.MAX_VALUE;
		private static final int BITS = 16;

		ToInt16Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.INT16, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			if (!branchFree())
				return castChecked(in, reader, MIN, MAX, BITS);

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final short[] dst = (short[])out.getData();

			boolean sawNaN = false;
			boolean sawOutOfRange = false;

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final double r = round(v);
				sawNaN |= v != v;
				sawOutOfRange |= outside(r, MIN, MAX);
				dst[i] = (short)(int)clamp(r, MIN, MAX);
			}

			if (mustReport(sawNaN, sawOutOfRange))
				return castChecked(in, reader, MIN, MAX, BITS);

			return out;
		}
	}

	private static final class ToUint16Caster extends Caster {

		private static final double MIN = 0;
		private static final double MAX = 0xffffL;
		private static final int BITS = 16;

		ToUint16Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.UINT16, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			if (!branchFree())
				return castChecked(in, reader, MIN, MAX, BITS);

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final short[] dst = (short[])out.getData();

			boolean sawNaN = false;
			boolean sawOutOfRange = false;

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final double r = round(v);
				sawNaN |= v != v;
				sawOutOfRange |= outside(r, MIN, MAX);
				dst[i] = (short)(int)clamp(r, MIN, MAX);
			}

			if (mustReport(sawNaN, sawOutOfRange))
				return castChecked(in, reader, MIN, MAX, BITS);

			return out;
		}
	}

	private static final class ToInt32Caster extends Caster {

		private static final double MIN = Integer.MIN_VALUE;
		private static final double MAX = Integer.MAX_VALUE;
		private static final int BITS = 32;

		ToInt32Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.INT32, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			if (!branchFree())
				return castChecked(in, reader, MIN, MAX, BITS);

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final int[] dst = (int[])out.getData();

			boolean sawNaN = false;
			boolean sawOutOfRange = false;

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final double r = round(v);
				sawNaN |= v != v;
				sawOutOfRange |= outside(r, MIN, MAX);
				dst[i] = (int)clamp(r, MIN, MAX);
			}

			if (mustReport(sawNaN, sawOutOfRange))
				return castChecked(in, reader, MIN, MAX, BITS);

			return out;
		}
	}

	private static final class ToUint32Caster extends Caster {

		private static final double MIN = 0;
		private static final double MAX = 0xffffffffL;
		private static final int BITS = 32;

		ToUint32Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.UINT32, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			if (!branchFree())
				return castChecked(in, reader, MIN, MAX, BITS);

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final int[] dst = (int[])out.getData();

			boolean sawNaN = false;
			boolean sawOutOfRange = false;

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final double r = round(v);
				sawNaN |= v != v;
				sawOutOfRange |= outside(r, MIN, MAX);
				// 2^32 - 1 saturates d2i, so this one needs the long
				dst[i] = (int)(long)clamp(r, MIN, MAX);
			}

			if (mustReport(sawNaN, sawOutOfRange))
				return castChecked(in, reader, MIN, MAX, BITS);

			return out;
		}
	}

	private static final class ToInt64Caster extends Caster {

		private static final double MIN = -0x1.0p63;
		/** 2<sup>63</sup> is not representable; use the largest double below it. */
		private static final double MAX = Math.nextDown(0x1.0p63);
		private static final int BITS = 64;

		ToInt64Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.INT64, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			if (!branchFree())
				return castChecked(in, reader, MIN, MAX, BITS);

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final long[] dst = (long[])out.getData();

			boolean sawNaN = false;
			boolean sawOutOfRange = false;

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final double r = round(v);
				sawNaN |= v != v;
				sawOutOfRange |= outside(r, MIN, MAX);
				dst[i] = (long)clamp(r, MIN, MAX);
			}

			if (mustReport(sawNaN, sawOutOfRange))
				return castChecked(in, reader, MIN, MAX, BITS);

			return out;
		}
	}

	private static final class ToUint64Caster extends Caster {

		private static final double MIN = 0;
		/** 2<sup>64</sup> is not representable; use the largest double below it. */
		private static final double MAX = Math.nextDown(0x1.0p64);
		private static final int BITS = 64;

		ToUint64Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.UINT64, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			if (!branchFree())
				return castChecked(in, reader, MIN, MAX, BITS);

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final long[] dst = (long[])out.getData();

			boolean sawNaN = false;
			boolean sawOutOfRange = false;

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final double r = round(v);
				sawNaN |= v != v;
				sawOutOfRange |= outside(r, MIN, MAX);
				dst[i] = BlockElementAccess.doubleToUnsignedLong(clamp(r, MIN, MAX));
			}

			if (mustReport(sawNaN, sawOutOfRange))
				return castChecked(in, reader, MIN, MAX, BITS);

			return out;
		}
	}

	// ------------------------------------------------------------------
	// Floating-point targets
	// ------------------------------------------------------------------

	private static final class ToFloat32Caster extends Caster {

		ToFloat32Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.FLOAT32, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final float[] dst = (float[])out.getData();

			boolean sawOverflow = false;

			// The narrowing cast already yields the clamped answer: a finite
			// double beyond the float range becomes the correctly signed
			// infinity, which is exactly what CLAMP prescribes. NaN and genuine
			// infinities pass through untouched. So the value is always right,
			// and only the error policy needs to know an overflow happened.
			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final float f = (float)v;
				sawOverflow |= Float.isInfinite(f) & !Double.isInfinite(v) & (v == v);
				dst[i] = f;
			}

			if (mustReportOverflow(sawOverflow))
				return castCheckedFloat32(in, reader);

			return out;
		}

		/** Re-runs scalar so the exception names the offending value. */
		private DataBlock<?> castCheckedFloat32(final DataBlock<?> in, final DoubleReader reader)
				throws N5IOException {

			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final float[] dst = (float[])out.getData();

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++) {
				final double v = reader.get(src, i);
				final float f = (float)v;
				if (Float.isInfinite(f) && !Double.isInfinite(v) && !Double.isNaN(v))
					throw outOfRangeException(v);
				dst[i] = f;
			}

			return out;
		}
	}

	private static final class ToFloat64Caster extends Caster {

		ToFloat64Caster(final Rounding rounding, final OutOfRange outOfRange) {

			super(DataType.FLOAT64, rounding, outOfRange);
		}

		@Override
		DataBlock<?> cast(final DataBlock<?> in, final DoubleReader reader) throws N5IOException {

			// every double is representable in float64; no rounding, no checks
			final DataBlock<?> out = newBlock(in);
			final Object src = in.getData();
			final double[] dst = (double[])out.getData();

			final int n = in.getNumElements();
			for (int i = 0; i < n; i++)
				dst[i] = reader.get(src, i);

			return out;
		}
	}
}
