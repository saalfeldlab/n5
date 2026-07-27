package org.janelia.saalfeldlab.n5.codec.dataset;

import java.util.function.DoubleUnaryOperator;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.DatasetCodec;
import org.janelia.saalfeldlab.n5.codec.dataset.BlockElementAccess.DoubleReader;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.Rounding;

/**
 * An "array -&gt; array" codec that fuses a {@code scale_offset}, a saturating
 * clamp to an arbitrary band, and a narrowing cast into a single element-wise
 * pass.
 * <p>
 * Encoding maps each value {@code v} of the source type through
 * <pre>    out = narrow(round(clamp((v - offset) * scale, clampMin, clampMax)))</pre>
 * and stores it as the target {@code data_type}. Decoding inverts the affine part
 * only &mdash; {@code (in / scale) + offset} &mdash; because the clamp is lossy
 * and cannot be undone, exactly as for a plain {@code scale_offset}.
 * <p>
 * <b>Why this exists.</b> The same result can be assembled from the standard
 * codecs, but only awkwardly: {@code cast_value}'s {@code clamp} saturates to the
 * target type's <em>full</em> range, so clamping to a band strictly inside that
 * range (e.g. {@code [-64, 64]} of an {@code int8}) requires borrowing a wider
 * integer type's limits and scaling back &mdash; four passes and three
 * intermediate allocations (see {@code ClampAndCastBenchmarks}). This codec does
 * it in one pass and one allocation.
 * <p>
 * <b>Why it is fast.</b> The clamp band is required to lie within the target
 * type's range, so after clamping every value provably fits: the encode loop
 * needs no per-element range check, no NaN flag, and no fallback path &mdash; the
 * machinery that keeps {@link CastValueCodec} scalar. What remains is affine
 * arithmetic, a branchless clamp (a ternary, which lowers to a conditional move
 * rather than a mispredicting branch), a rounding intrinsic and a narrowing cast,
 * all of which vectorize.
 * <p>
 * <b>Boundary behavior.</b> {@code ±Infinity} saturates to {@code clampMax} /
 * {@code clampMin}. {@code NaN} narrows to {@code 0} for an integral target and
 * is preserved for a float target; encode this explicitly (via a fill value or a
 * scalar map) if a different mapping is required. The narrowing follows the same
 * width rules as {@link CastValueCodec}: 8-, 16-
 * and signed-32-bit targets narrow through {@code (int)} to avoid the {@code d2l}
 * saturation penalty, {@code uint32} keeps {@code (long)} because
 * 2<sup>32</sup>-1 saturates {@code d2i}, and 64-bit targets take the full
 * {@code long}.
 *
 * @param <S>
 *            the source (decoded) block data type
 * @param <T>
 *            the target (encoded) block data type
 */
public class ScaleOffsetCastCodec<S, T> implements DatasetCodec<S, T> {

	private final DataType sourceDataType;

	private final DataType targetDataType;

	private final double scale;

	private final double offset;

	/** Clamp band, in output (post-scale) units; must lie within the target type's range. */
	private final double clampMin;

	private final double clampMax;

	private final Rounding rounding;

	/** Reads the source type; used when encoding. */
	private final DoubleReader sourceReader;

	/** Reads the target type; used when decoding. */
	private final DoubleReader targetReader;

	/** Rounding mode resolved once; monomorphic per instance, so it inlines. */
	private final DoubleUnaryOperator rounder;

	public ScaleOffsetCastCodec(
			final DataType sourceDataType,
			final DataType targetDataType,
			final double scale,
			final double offset,
			final double clampMin,
			final double clampMax,
			final Rounding rounding) {

		this.sourceDataType = sourceDataType;
		this.targetDataType = targetDataType;
		this.scale = scale;
		this.offset = offset;
		this.clampMin = clampMin;
		this.clampMax = clampMax;
		this.rounding = rounding == null ? CastValueCodecInfo.DEFAULT_ROUNDING : rounding;

		this.sourceReader = BlockElementAccess.reader(sourceDataType);
		this.targetReader = BlockElementAccess.reader(targetDataType);
		this.rounder = Rounders.forRounding(this.rounding);
	}

	public DataType getSourceDataType() {

		return sourceDataType;
	}

	public DataType getTargetDataType() {

		return targetDataType;
	}

	public double getScale() {

		return scale;
	}

	public double getOffset() {

		return offset;
	}

	public double getClampMin() {

		return clampMin;
	}

	public double getClampMax() {

		return clampMax;
	}

	public Rounding getRounding() {

		return rounding;
	}

	// ------------------------------------------------------------------
	// Element math. These are small and monomorphic, so C2 inlines them
	// into the loops below, leaving a straight-line, vectorizable body.
	// ------------------------------------------------------------------

	/** {@code (v - offset) * scale}. */
	private double affine(final double v) {

		return (v - offset) * scale;
	}

	/**
	 * Clamps {@code s} into {@code [clampMin, clampMax]}.
	 * <p>
	 * {@link Math#min}/{@link Math#max} are used deliberately: they lower to the
	 * vectorized {@code vminpd}/{@code vmaxpd} instructions, whereas the obvious
	 * nested ternary compiles to real branches that mispredict on out-of-band
	 * values. Measured (see {@code ClampAndCastBenchmarks}), the ternary was 2.3&times;
	 * slower on a fill with a realistic fraction of clamped values.
	 * <p>
	 * {@code NaN} propagates through both calls and narrows to {@code 0} for an
	 * integral target (and is preserved for a float target); {@code ±Infinity}
	 * saturates to {@code clampMax}/{@code clampMin}.
	 */
	private double clamp(final double s) {

		return Math.min(clampMax, Math.max(clampMin, s));
	}

	/** Encode transform for integral targets: {@code round(clamp(affine(v)))}. */
	private double quantize(final double v) {

		return rounder.applyAsDouble(clamp(affine(v)));
	}

	// ------------------------------------------------------------------
	// Encode: source -> target, with scale/offset, clamp and narrowing.
	// ------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public DataBlock<T> encode(final DataBlock<S> block) throws N5IOException {

		final DataBlock<?> out = targetDataType.createDataBlock(
				block.getSize(), block.getGridPosition(), block.getNumElements());

		final Object src = block.getData();
		final Object dst = out.getData();
		final DoubleReader reader = sourceReader;
		final int n = block.getNumElements();

		switch (targetDataType) {
		case INT8:
		case UINT8: {
			final byte[] d = (byte[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (byte)(int)quantize(reader.get(src, i));
			break;
		}
		case INT16:
		case UINT16: {
			final short[] d = (short[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (short)(int)quantize(reader.get(src, i));
			break;
		}
		case INT32: {
			final int[] d = (int[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (int)quantize(reader.get(src, i));
			break;
		}
		case UINT32: {
			final int[] d = (int[])dst;
			for (int i = 0; i < n; i++)
				// 2^32 - 1 saturates d2i, so this one needs the long
				d[i] = (int)(long)quantize(reader.get(src, i));
			break;
		}
		case INT64: {
			final long[] d = (long[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (long)quantize(reader.get(src, i));
			break;
		}
		case UINT64: {
			final long[] d = (long[])dst;
			for (int i = 0; i < n; i++)
				d[i] = BlockElementAccess.doubleToUnsignedLong(quantize(reader.get(src, i)));
			break;
		}
		case FLOAT32: {
			// float target: clamp to the band, but do not round to an integer
			final float[] d = (float[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (float)clamp(affine(reader.get(src, i)));
			break;
		}
		case FLOAT64: {
			final double[] d = (double[])dst;
			for (int i = 0; i < n; i++)
				d[i] = clamp(affine(reader.get(src, i)));
			break;
		}
		default:
			throw new N5IOException("ScaleOffsetCastCodec cannot cast to data type " + targetDataType);
		}

		return (DataBlock<T>)out;
	}

	// ------------------------------------------------------------------
	// Decode: target -> source, inverting the affine map only. The clamp
	// is lossy and is not re-applied, as for a plain scale_offset.
	// ------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	@Override
	public DataBlock<S> decode(final DataBlock<T> block) throws N5IOException {

		final DataBlock<?> out = sourceDataType.createDataBlock(
				block.getSize(), block.getGridPosition(), block.getNumElements());

		final Object src = block.getData();
		final Object dst = out.getData();
		final DoubleReader reader = targetReader;
		final int n = block.getNumElements();

		switch (sourceDataType) {
		case INT8:
		case UINT8: {
			final byte[] d = (byte[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (byte)(int)rounder.applyAsDouble(invert(reader.get(src, i)));
			break;
		}
		case INT16:
		case UINT16: {
			final short[] d = (short[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (short)(int)rounder.applyAsDouble(invert(reader.get(src, i)));
			break;
		}
		case INT32: {
			final int[] d = (int[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (int)rounder.applyAsDouble(invert(reader.get(src, i)));
			break;
		}
		case UINT32: {
			final int[] d = (int[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (int)(long)rounder.applyAsDouble(invert(reader.get(src, i)));
			break;
		}
		case INT64: {
			final long[] d = (long[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (long)rounder.applyAsDouble(invert(reader.get(src, i)));
			break;
		}
		case UINT64: {
			final long[] d = (long[])dst;
			for (int i = 0; i < n; i++)
				d[i] = BlockElementAccess.doubleToUnsignedLong(rounder.applyAsDouble(invert(reader.get(src, i))));
			break;
		}
		case FLOAT32: {
			final float[] d = (float[])dst;
			for (int i = 0; i < n; i++)
				d[i] = (float)invert(reader.get(src, i));
			break;
		}
		case FLOAT64: {
			final double[] d = (double[])dst;
			for (int i = 0; i < n; i++)
				d[i] = invert(reader.get(src, i));
			break;
		}
		default:
			throw new N5IOException("ScaleOffsetCastCodec cannot decode to data type " + sourceDataType);
		}

		return (DataBlock<S>)out;
	}

	/** {@code (v / scale) + offset}, the inverse of {@link #affine}. */
	private double invert(final double v) {

		return (v / scale) + offset;
	}
}
