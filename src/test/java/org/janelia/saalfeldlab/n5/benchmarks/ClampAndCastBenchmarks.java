package org.janelia.saalfeldlab.n5.benchmarks;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodec;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.OutOfRange;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.Rounding;
import org.janelia.saalfeldlab.n5.codec.dataset.ScaleOffsetCastCodec;
import org.janelia.saalfeldlab.n5.codec.dataset.ScaleOffsetCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmarks the "clamp to a sub-range, then narrow" operation expressed as a
 * chain of existing codecs, so it can later be compared head-to-head against a
 * single fused codec that does the same thing in one pass.
 * <p>
 * <b>The task.</b> {@code float64} values in {@code [-22, 24]} are mapped so that
 * {@code [-20, 20]} lands linearly on {@code [-64, 64]} of an {@code int8}, and
 * anything outside {@code [-20, 20]} saturates at {@code ±64}. Neither
 * {@code scale_offset} nor {@code cast_value} can clamp to a band strictly inside
 * the target type's range on its own &mdash; {@code cast_value}'s {@code clamp}
 * always saturates to the type's <em>full</em> range &mdash; so the clamp has to
 * be borrowed from a wider integer type's limits and then scaled back down:
 * <ol>
 * <li>{@code scale_offset(32767/20)}: blow the clamp threshold {@code ±20} up
 * onto {@code int16}'s limit {@code ±32767}. ({@code float64 -> float64})</li>
 * <li>{@code cast_value(int16, clamp)}: saturate the tails at {@code int16}'s
 * range. ({@code float64 -> int16})</li>
 * <li>{@code scale_offset(64/32767)}: bring the clamped band down into
 * {@code [-64, 64]}. ({@code int16 -> int16})</li>
 * <li>{@code cast_value(int8)}: narrow to the final type; already in range, so no
 * policy needed. ({@code int16 -> int8})</li>
 * </ol>
 * The two scale factors compose to {@code 64/20 = 3.2}, so the interior mapping
 * is exactly the plain {@code scale_offset(3.2) -> cast_value(int8)} pipeline;
 * the two middle codecs exist only to bound the tails. This costs four
 * element-wise passes and three intermediate block allocations to accomplish
 * what is conceptually one clamp-and-cast &mdash; which is the motivation for the
 * fused codec.
 * <p>
 * <b>What to compare against.</b> {@link #baselineAllocateTarget} is the
 * allocate-and-copy floor for the {@code int8} output. The eventual fused-codec
 * benchmark belongs in this class so the two run under one JMH invocation and
 * share warmup conditions.
 * <p>
 * <b>Read the control first.</b> As with the other codec benchmarks, these
 * numbers are only comparable across runs on a quiet machine &mdash; see
 * {@code CODEC_NOTES.md} §6. The per-stage benchmarks below double as controls:
 * {@link #stage1ScaleOffsetUp} is a plain {@code float64} {@code scale_offset}
 * and should track {@code DatasetCodecBenchmarks.scaleOffsetEncode} on
 * {@code float64}.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class ClampAndCastBenchmarks {

	@Param(value = {"64"})
	protected int blockDim;

	@Param(value = {"3"})
	protected int numDimensions;

	private static final DataType SOURCE_TYPE = DataType.FLOAT64;
	private static final DataType CLAMP_TYPE = DataType.INT16;
	private static final DataType TARGET_TYPE = DataType.INT8;

	/** Input band the caller cares about; clamp thresholds are its edges. */
	private static final double IN_LO = -20.0;
	private static final double IN_HI = 20.0;

	/** Output band {@code [-20, 20]} is linearly mapped onto. */
	private static final double OUT_ABS = 64.0;

	/** Blow {@code ±IN_HI} up onto {@code int16}'s limit, so the clamp bites there. */
	private static final double SCALE_UP = Short.MAX_VALUE / IN_HI;

	/** Bring the clamped {@code int16} band down into {@code [-OUT_ABS, OUT_ABS]}. */
	private static final double SCALE_DOWN = OUT_ABS / Short.MAX_VALUE;

	/** Overall linear map the chain realizes: (outHi-outLo)/(inHi-inLo) = 3.2. */
	private static final double NET_SCALE = (2 * OUT_ABS) / (IN_HI - IN_LO);

	// Constants for the hand-written probe loops below, matching {@link #fused}.
	private static final double OFFSET = 0.0;
	private static final double CMIN = -OUT_ABS;
	private static final double CMAX = OUT_ABS;

	private ScaleOffsetCodec<Object> scaleUp;
	private CastValueCodec<Object, Object> clampToInt16;
	private ScaleOffsetCodec<Object> scaleDown;
	private CastValueCodec<Object, Object> castToInt8;

	/** Single-pass equivalent of the whole four-codec chain. */
	private ScaleOffsetCastCodec<Object, Object> fused;

	/** Decoded block, {@code float64}, filled across {@code [-22, 24]}. */
	private DataBlock<Object> sourceBlock;

	public static void main(final String... args) throws RunnerException {

		final Options options = new OptionsBuilder()
				.include(ClampAndCastBenchmarks.class.getSimpleName() + "\\.")
				.build();

		new Runner(options).run();
	}

	@SuppressWarnings("unchecked")
	@Setup(Level.Trial)
	public void setup() {

		final int[] blockSize = new int[numDimensions];
		Arrays.fill(blockSize, blockDim);
		final long[] gridPosition = new long[numDimensions];

		scaleUp = new ScaleOffsetCodec<>(SOURCE_TYPE, SCALE_UP, 0.0);
		clampToInt16 = new CastValueCodec<>(SOURCE_TYPE, CLAMP_TYPE, Rounding.NEAREST_EVEN, OutOfRange.CLAMP);
		scaleDown = new ScaleOffsetCodec<>(CLAMP_TYPE, SCALE_DOWN, 0.0);
		castToInt8 = new CastValueCodec<>(CLAMP_TYPE, TARGET_TYPE, Rounding.NEAREST_EVEN, null);

		fused = new ScaleOffsetCastCodec<>(
				SOURCE_TYPE, TARGET_TYPE, NET_SCALE, 0.0, -OUT_ABS, OUT_ABS, Rounding.NEAREST_EVEN);

		sourceBlock = (DataBlock<Object>)SOURCE_TYPE.createDataBlock(blockSize, gridPosition);

		// Fill across [-22, 24]: a chunk of every value lands outside [-20, 20]
		// and must be clamped, so the tail-handling path is actually exercised.
		final double[] data = (double[])sourceBlock.getData();
		final Random random = new Random(7777);
		for (int i = 0; i < data.length; i++)
			data[i] = -22.0 + random.nextDouble() * (24.0 - -22.0);
	}

	/** The full four-codec clamp-and-cast chain; the number the fused codec must beat. */
	@Benchmark
	public void chainEncode(final Blackhole hole) {

		hole.consume(
				castToInt8.encode(
						scaleDown.encode(
								clampToInt16.encode(
										scaleUp.encode(sourceBlock)))));
	}

	/** The one-pass fused codec; should land near the single-pass floor, not the chain. */
	@Benchmark
	public void fusedEncode(final Blackhole hole) {

		hole.consume(fused.encode(sourceBlock));
	}

	// ------------------------------------------------------------------
	// Probe variants: hand-written float64 -> int8 loops that share one
	// read + rint mechanism and differ only in the clamp/round axis, to
	// isolate what in the fused loop is not collapsing to vectorized code.
	// fused_ternary is the controlled baseline and should reproduce
	// fusedEncode; each other variant removes or swaps exactly one thing.
	// ------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private DataBlock<Object> newTargetBlock() {

		return (DataBlock<Object>)TARGET_TYPE.createDataBlock(
				sourceBlock.getSize(), sourceBlock.getGridPosition(), sourceBlock.getNumElements());
	}

	/** round(ternary-clamp(affine)) — the controlled reproduction of {@link #fused}. */
	@Benchmark
	public void fused_ternary(final Blackhole hole) {

		final double[] src = (double[])sourceBlock.getData();
		final DataBlock<Object> out = newTargetBlock();
		final byte[] d = (byte[])out.getData();
		for (int i = 0; i < src.length; i++) {
			final double s = (src[i] - OFFSET) * NET_SCALE;
			final double c = s > CMAX ? CMAX : (s >= CMIN ? s : CMIN);
			d[i] = (byte)(int)Math.rint(c);
		}
		hole.consume(out);
	}

	/** ternary-clamp(affine), no rint — isolates the rounding intrinsic. */
	@Benchmark
	public void fused_noround(final Blackhole hole) {

		final double[] src = (double[])sourceBlock.getData();
		final DataBlock<Object> out = newTargetBlock();
		final byte[] d = (byte[])out.getData();
		for (int i = 0; i < src.length; i++) {
			final double s = (src[i] - OFFSET) * NET_SCALE;
			final double c = s > CMAX ? CMAX : (s >= CMIN ? s : CMIN);
			d[i] = (byte)(int)c;
		}
		hole.consume(out);
	}

	/** round(Math.min/max-clamp(affine)) — the suspected regression culprit vs the ternary. */
	@Benchmark
	public void fused_minmax(final Blackhole hole) {

		final double[] src = (double[])sourceBlock.getData();
		final DataBlock<Object> out = newTargetBlock();
		final byte[] d = (byte[])out.getData();
		for (int i = 0; i < src.length; i++) {
			final double s = (src[i] - OFFSET) * NET_SCALE;
			final double c = Math.min(CMAX, Math.max(CMIN, s));
			d[i] = (byte)(int)Math.rint(c);
		}
		hole.consume(out);
	}

	/** round(affine), no clamp — isolates the clamp (fill stays in int8 range, so this is safe). */
	@Benchmark
	public void fused_noclamp(final Blackhole hole) {

		final double[] src = (double[])sourceBlock.getData();
		final DataBlock<Object> out = newTargetBlock();
		final byte[] d = (byte[])out.getData();
		for (int i = 0; i < src.length; i++) {
			final double s = (src[i] - OFFSET) * NET_SCALE;
			d[i] = (byte)(int)Math.rint(s);
		}
		hole.consume(out);
	}

	@Benchmark
	public void stage1ScaleOffsetUp(final Blackhole hole) {

		hole.consume(scaleUp.encode(sourceBlock));
	}

	/** Allocate-and-copy floor for the {@code int8} output. */
	@Benchmark
	public void baselineAllocateTarget(final Blackhole hole) {

		hole.consume(TARGET_TYPE.createDataBlock(
				sourceBlock.getSize(), sourceBlock.getGridPosition(), sourceBlock.getNumElements()));
	}

}
