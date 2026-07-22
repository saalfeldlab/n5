package org.janelia.saalfeldlab.n5.benchmarks;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodec;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.OutOfRange;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.Rounding;
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
 * Benchmarks {@code cast_value} across the axes along which its implementation
 * actually varies. {@link DatasetCodecBenchmarks} covers {@code cast_value} only
 * at {@code nearest-even} / {@code clamp}, which exercises one of the fifteen
 * (rounding, out-of-range) combinations and one of the code paths.
 * <p>
 * Each axis corresponds to something the branch-free rewrite treats differently,
 * so a regression can hide in any of them:
 * <dl>
 * <dt>{@code rounding}</dt>
 * <dd>Resolved to a {@code DoubleUnaryOperator} once, rather than switched per
 * element. {@code nearest-even} becomes {@code Math::rint}, which is a single
 * instruction; {@code towards-zero} becomes a lambda containing a sign test.
 * That test is a branch <em>inside</em> the element loop, and per-element
 * branching is precisely what made the old implementation slow — so the two
 * modes may not perform alike.</dd>
 *
 * <dt>{@code outOfRange}</dt>
 * <dd>{@code clamp} and the default error policy share one branch-free loop;
 * {@code wrap} needs modular arithmetic and delegates to the scalar
 * {@code castChecked}. So {@code wrap} is expected to be slower — this measures
 * by how much, and confirms it did not regress relative to the pre-refactor
 * code, which was scalar for every policy.</dd>
 *
 * <dt>{@code castDataType}</dt>
 * <dd>8-, 16- and signed-32-bit targets narrow with {@code (int)}; {@code uint32}
 * must keep {@code (long)} because 2<sup>32</sup>-1 saturates {@code d2i}. That
 * narrowing measured ~23% of the codec's overhead, so {@code uint32} should sit
 * measurably above {@code int8}/{@code int16}.</dd>
 * </dl>
 * <p>
 * The source type is fixed to {@code float64}: it is the widest input and does
 * the most conversion work, and {@link DatasetCodecBenchmarks} already varies
 * the source axis. Only encoding is measured — decoding here would write
 * {@code float64}, whose caster is a bare copy loop with no rounding, range
 * check or policy, so it exercises none of the axes above.
 * <p>
 * The fill is in {@code [0, 100)}, in range for every target, so the error
 * policy never throws and no variant takes its re-check path. This measures the
 * hot path, which is the point; the error path ends in an exception and its cost
 * is irrelevant.
 * <p>
 * <b>Read the control first.</b> These numbers are only comparable across runs
 * if the machine was quiet — see {@code CODEC_NOTES.md} §6. Run
 * {@link DatasetCodecBenchmarks} alongside and check {@code scaleOffsetEncode}
 * on {@code int16} is ≈110 µs before trusting anything here.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class CastValueCodecBenchmarks {

	/** Narrowing target. Chosen to span the three narrowing widths. */
	@Param(value = {"int8", "int16", "uint32"})
	protected String castDataType;

	/** {@code nearest-even} is branch-free; {@code towards-zero} is not. */
	@Param(value = {"nearest-even", "towards-zero"})
	protected String rounding;

	/** {@code none} means the default error policy. */
	@Param(value = {"none", "clamp", "wrap"})
	protected String outOfRange;

	@Param(value = {"64"})
	protected int blockDim;

	private static final DataType SOURCE_TYPE = DataType.FLOAT64;
	private static final int NUM_DIMENSIONS = 3;

	private DataType targetType;

	private CastValueCodec<Object, Object> codec;
	private DataBlock<Object> sourceBlock;

	public static void main(final String... args) throws RunnerException {

		final Options options = new OptionsBuilder()
				.include(CastValueCodecBenchmarks.class.getSimpleName() + "\\.")
				.build();

		new Runner(options).run();
	}

	@SuppressWarnings("unchecked")
	@Setup(Level.Trial)
	public void setup() {

		targetType = DataType.fromString(castDataType);

		final int[] blockSize = new int[NUM_DIMENSIONS];
		Arrays.fill(blockSize, blockDim);
		final long[] gridPosition = new long[NUM_DIMENSIONS];

		codec = new CastValueCodec<>(
				SOURCE_TYPE,
				targetType,
				Rounding.fromString(rounding),
				"none".equals(outOfRange) ? null : OutOfRange.fromString(outOfRange));

		sourceBlock = (DataBlock<Object>)SOURCE_TYPE.createDataBlock(blockSize, gridPosition);

		final double[] data = (double[])sourceBlock.getData();
		final Random random = new Random(7777);
		for (int i = 0; i < data.length; i++)
			data[i] = random.nextDouble() * 100.0;
	}

	@Benchmark
	public void encode(final Blackhole hole) {

		hole.consume(codec.encode(sourceBlock));
	}

	/** Allocation and bandwidth floor for the target block. */
	@Benchmark
	public void baselineAllocate(final Blackhole hole) {

		hole.consume(targetType.createDataBlock(
				sourceBlock.getSize(), sourceBlock.getGridPosition(), sourceBlock.getNumElements()));
	}

}
