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
 * Both codecs allocate a fresh block per call and do their arithmetic in
 * {@code double}, so {@link #baselineAllocateAndCopy} is included as a
 * reference point: it allocates a block of the same type and {@code
 * System.arraycopy}s into it, isolating allocation and memory bandwidth from
 * the per-element conversion cost.
 * <p>
 * The {@code pipeline*} benchmarks measure the canonical lossy-compression
 * chain, {@code scale_offset} followed by {@code cast_value}.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class DatasetCodecBenchmarks {

	/** Data type of the (decoded) dataset. */
	@Param(value = {"float64", "float32"})
	protected String dataType;

	/** Narrower data type that {@code cast_value} encodes to. */
	@Param(value = {"int16"})
	protected String castDataType;

	@Param(value = {"64"})
	protected int blockDim;

	@Param(value = {"3"})
	protected int numDimensions;

	private static final double SCALE = 2.0;
	private static final double OFFSET = 50.0;

	private final Random random = new Random(7777);

	private DataType sourceType;
	private DataType targetType;

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

		final int[] blockSize = new int[numDimensions];
		Arrays.fill(blockSize, blockDim);
		final long[] gridPosition = new long[numDimensions];

		scaleOffset = new ScaleOffsetCodec<>(sourceType, SCALE, OFFSET);
		castValue = new CastValueCodec<>(sourceType, targetType, Rounding.NEAREST_EVEN, OutOfRange.CLAMP);

		sourceBlock = (DataBlock<Object>)sourceType.createDataBlock(blockSize, gridPosition);
		fill(sourceBlock.getData());

		// pre-encode the inputs the decode benchmarks need
		scaledBlock = scaleOffset.encode(sourceBlock);
		castBlock = castValue.encode(sourceBlock);
		pipelineBlock = castValue.encode(scaledBlock);
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
	 * Reference point: allocate a block of the source type and copy into it,
	 * with no per-element conversion.
	 */
	@Benchmark
	public void baselineAllocateAndCopy(final Blackhole hole) {

		final DataBlock<?> out = sourceType.createDataBlock(
				sourceBlock.getSize(), sourceBlock.getGridPosition(), sourceBlock.getNumElements());
		System.arraycopy(sourceBlock.getData(), 0, out.getData(), 0, sourceBlock.getNumElements());
		hole.consume(out);
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

}
