package org.janelia.saalfeldlab.n5.benchmarks;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodec;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.OutOfRange;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.Rounding;
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
 * Benchmarks for the {@code scale_offset} and {@code cast_value} dataset
 * codecs, which transform a {@link DataBlock} element-wise.
 * <p>
 * Both codecs allocate a fresh block per call, and for a 64<sup>3</sup> block
 * that allocation is not negligible &mdash; so two baselines are provided.
 * {@link #baselineAllocateSource} allocates a block of the <em>source</em> type
 * and {@code System.arraycopy}s into it; {@link #baselineAllocateTarget} does
 * the same for the <em>cast target</em> type. A codec benchmark should be
 * compared against the baseline matching the type it <em>writes</em>, since
 * that is the allocation it actually performs. Comparing a narrowing cast
 * against the source-type baseline understates its per-element cost, sometimes
 * by a factor of four.
 * <p>
 * The {@code pipeline*} benchmarks measure the canonical lossy-compression
 * chain, {@code scale_offset} followed by {@code cast_value}.
 * <p>
 * <b>On {@link #setup}:</b> the pre-encoded blocks that the {@code *Decode}
 * benchmarks consume are built by the private helpers at the bottom of this
 * class rather than by calling the codecs. This is deliberate and load-bearing.
 * JMH runs each benchmark method in its own fork, but {@code setup} runs in
 * <em>every</em> fork &mdash; so pre-encoding via the codec would drive
 * {@code CastValueCodec.cast} with several different source/target type pairs
 * before the measured loop ever starts. The reader/writer call sites inside
 * that one shared method would then be polymorphic, defeating inlining and
 * charging the measured benchmark for dispatch it would not perform in
 * production. Building the fixtures independently keeps each fork's view of the
 * codec monomorphic. Do not "simplify" these helpers back into codec calls.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class DatasetCodecBenchmarks {

	/**
	 * Data type of the (decoded) dataset. The integral types matter as much as
	 * the floating-point ones: for an integral source the codecs' {@code double}
	 * pivot is pure overhead, so those rows isolate dispatch cost from
	 * arithmetic cost.
	 */
	@Param(value = {"float64", "float32", "int16", "uint8"})
	protected String dataType;

	/** Data type that {@code cast_value} encodes to. */
	@Param(value = {"int16"})
	protected String castDataType;

	@Param(value = {"64"})
	protected int blockDim;

	@Param(value = {"3"})
	protected int numDimensions;

	private static final double SCALE = 2.0;

	private final Random random = new Random(7777);

	private DataType sourceType;
	private DataType targetType;

	/**
	 * Chosen per source type so that no intermediate value leaves the source
	 * type's range; see {@link #offsetFor}.
	 */
	private double offset;

	private ScaleOffsetCodec<Object> scaleOffset;
	private CastValueCodec<Object, Object> castValue;

	/** Decoded block, in the source data type. */
	private DataBlock<Object> sourceBlock;

	/** {@code scale_offset}-encoded block (still the source data type). */
	private DataBlock<Object> scaledBlock;

	/** {@code cast_value}-encoded block, in the target data type. */
	private DataBlock<Object> castBlock;

	/** {@code scale_offset} then {@code cast_value} encoded block. */
	private DataBlock<Object> pipelineBlock;

	public static void main(final String... args) throws RunnerException {

		final Options options = new OptionsBuilder()
				.include(DatasetCodecBenchmarks.class.getSimpleName() + "\\.")
				.build();

		new Runner(options).run();
	}

	@SuppressWarnings("unchecked")
	@Setup(Level.Trial)
	public void setup() {

		sourceType = DataType.fromString(dataType);
		targetType = DataType.fromString(castDataType);
		offset = offsetFor(sourceType);

		final int[] blockSize = new int[numDimensions];
		Arrays.fill(blockSize, blockDim);
		final long[] gridPosition = new long[numDimensions];

		scaleOffset = new ScaleOffsetCodec<>(sourceType, SCALE, offset);
		castValue = new CastValueCodec<>(sourceType, targetType, Rounding.NEAREST_EVEN, OutOfRange.CLAMP);

		sourceBlock = (DataBlock<Object>)sourceType.createDataBlock(blockSize, gridPosition);
		fill(sourceBlock.getData());

		// Fixtures for the *Decode benchmarks, built WITHOUT touching the
		// codecs -- see the class javadoc.
		final double[] source = toDoubles(sourceBlock.getData(), sourceType);

		final double[] scaled = new double[source.length];
		for (int i = 0; i < source.length; i++)
			scaled[i] = (source[i] - offset) * SCALE;

		scaledBlock = fromDoubles(scaled, sourceType, blockSize, gridPosition);
		castBlock = fromDoubles(source, targetType, blockSize, gridPosition);
		pipelineBlock = fromDoubles(scaled, targetType, blockSize, gridPosition);
	}

	@Benchmark
	public void scaleOffsetEncode(final Blackhole hole) {

		hole.consume(scaleOffset.encode(sourceBlock));
	}

	@Benchmark
	public void scaleOffsetDecode(final Blackhole hole) {

		hole.consume(scaleOffset.decode(scaledBlock));
	}

	@Benchmark
	public void castValueEncode(final Blackhole hole) {

		hole.consume(castValue.encode(sourceBlock));
	}

	@Benchmark
	public void castValueDecode(final Blackhole hole) {

		hole.consume(castValue.decode(castBlock));
	}

	@Benchmark
	public void pipelineEncode(final Blackhole hole) {

		hole.consume(castValue.encode(scaleOffset.encode(sourceBlock)));
	}

	@Benchmark
	public void pipelineDecode(final Blackhole hole) {

		hole.consume(scaleOffset.decode(castValue.decode(pipelineBlock)));
	}

	/**
	 * Reference point for benchmarks that write the <em>source</em> type
	 * ({@code scaleOffset*}, {@code castValueDecode}): allocate and copy, with
	 * no per-element conversion.
	 */
	@Benchmark
	public void baselineAllocateSource(final Blackhole hole) {

		hole.consume(allocateAndCopy(sourceBlock, sourceType));
	}

	/**
	 * Reference point for benchmarks that write the <em>cast target</em> type
	 * ({@code castValueEncode}, {@code pipelineEncode}): allocate and copy, with
	 * no per-element conversion.
	 */
	@Benchmark
	public void baselineAllocateTarget(final Blackhole hole) {

		hole.consume(allocateAndCopy(castBlock, targetType));
	}

	private static DataBlock<?> allocateAndCopy(final DataBlock<?> in, final DataType type) {

		final DataBlock<?> out = type.createDataBlock(in.getSize(), in.getGridPosition(), in.getNumElements());
		System.arraycopy(in.getData(), 0, out.getData(), 0, in.getNumElements());
		return out;
	}

	/**
	 * Returns a {@code scale_offset} offset that keeps every intermediate value
	 * inside the source type's range, given the {@code [0, 100)} fill.
	 * <p>
	 * Unsigned types get {@code 0}: subtracting a positive offset would
	 * underflow, and {@link ScaleOffsetCodec} does not currently reject that, it
	 * silently wraps.
	 */
	private static double offsetFor(final DataType type) {

		switch (type) {
		case UINT8:
		case UINT16:
		case UINT32:
		case UINT64:
			return 0.0;
		default:
			return 50.0;
		}
	}

	/**
	 * Fills with values in {@code [0, 100)}, which stay in range for every
	 * supported cast target once {@code scale_offset} has been applied.
	 */
	private void fill(final Object data) {

		if (data instanceof double[]) {
			final double[] a = (double[])data;
			for (int i = 0; i < a.length; i++)
				a[i] = random.nextDouble() * 100.0;
		} else if (data instanceof float[]) {
			final float[] a = (float[])data;
			for (int i = 0; i < a.length; i++)
				a[i] = random.nextFloat() * 100.0f;
		} else if (data instanceof long[]) {
			final long[] a = (long[])data;
			for (int i = 0; i < a.length; i++)
				a[i] = random.nextInt(100);
		} else if (data instanceof int[]) {
			final int[] a = (int[])data;
			for (int i = 0; i < a.length; i++)
				a[i] = random.nextInt(100);
		} else if (data instanceof short[]) {
			final short[] a = (short[])data;
			for (int i = 0; i < a.length; i++)
				a[i] = (short)random.nextInt(100);
		} else if (data instanceof byte[]) {
			final byte[] a = (byte[])data;
			for (int i = 0; i < a.length; i++)
				a[i] = (byte)random.nextInt(100);
		} else {
			throw new IllegalArgumentException("Cannot fill data of type " + data.getClass());
		}
	}

	// ------------------------------------------------------------------
	// Fixture construction. Intentionally duplicates a little of what the
	// codecs do, so that building a fixture never executes codec code.
	// ------------------------------------------------------------------

	private static double[] toDoubles(final Object data, final DataType type) {

		final int n = java.lang.reflect.Array.getLength(data);
		final double[] out = new double[n];
		for (int i = 0; i < n; i++) {
			switch (type) {
			case INT8:
				out[i] = ((byte[])data)[i];
				break;
			case UINT8:
				out[i] = ((byte[])data)[i] & 0xff;
				break;
			case INT16:
				out[i] = ((short[])data)[i];
				break;
			case UINT16:
				out[i] = ((short[])data)[i] & 0xffff;
				break;
			case INT32:
				out[i] = ((int[])data)[i];
				break;
			case UINT32:
				out[i] = ((int[])data)[i] & 0xffffffffL;
				break;
			case INT64:
			case UINT64:
				out[i] = ((long[])data)[i];
				break;
			case FLOAT32:
				out[i] = ((float[])data)[i];
				break;
			case FLOAT64:
				out[i] = ((double[])data)[i];
				break;
			default:
				throw new IllegalArgumentException("No numerical access for " + type);
			}
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	private static DataBlock<Object> fromDoubles(
			final double[] values, final DataType type, final int[] blockSize, final long[] gridPosition) {

		final DataBlock<Object> block = (DataBlock<Object>)type.createDataBlock(
				blockSize, gridPosition, values.length);
		final Object data = block.getData();

		for (int i = 0; i < values.length; i++) {
			final double v = values[i];
			switch (type) {
			case INT8:
			case UINT8:
				((byte[])data)[i] = (byte)(long)clampRound(v, type);
				break;
			case INT16:
			case UINT16:
				((short[])data)[i] = (short)(long)clampRound(v, type);
				break;
			case INT32:
			case UINT32:
				((int[])data)[i] = (int)(long)clampRound(v, type);
				break;
			case INT64:
			case UINT64:
				((long[])data)[i] = (long)clampRound(v, type);
				break;
			case FLOAT32:
				((float[])data)[i] = (float)v;
				break;
			case FLOAT64:
				((double[])data)[i] = v;
				break;
			default:
				throw new IllegalArgumentException("No numerical access for " + type);
			}
		}
		return block;
	}

	private static double clampRound(final double value, final DataType type) {

		final double rounded = Math.rint(value);
		final double min;
		final double max;
		switch (type) {
		case INT8:
			min = Byte.MIN_VALUE;
			max = Byte.MAX_VALUE;
			break;
		case UINT8:
			min = 0;
			max = 0xffL;
			break;
		case INT16:
			min = Short.MIN_VALUE;
			max = Short.MAX_VALUE;
			break;
		case UINT16:
			min = 0;
			max = 0xffffL;
			break;
		case INT32:
			min = Integer.MIN_VALUE;
			max = Integer.MAX_VALUE;
			break;
		case UINT32:
			min = 0;
			max = 0xffffffffL;
			break;
		default:
			return rounded;
		}
		return Math.min(max, Math.max(min, rounded));
	}

}
