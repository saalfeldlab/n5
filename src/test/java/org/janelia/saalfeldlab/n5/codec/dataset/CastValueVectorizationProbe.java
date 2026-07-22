package org.janelia.saalfeldlab.n5.codec.dataset;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.dataset.BlockElementAccess.DoubleReader;
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
 * Diagnostic probe: why does {@code cast_value} cost ~330 &micro;s/block over
 * baseline regardless of what conversion it performs?
 * <p>
 * Lives in the codec's own package so it can reach package-private
 * {@link BlockElementAccess}. This is a throwaway experiment, not a regression
 * benchmark &mdash; delete it once the question is answered.
 * <p>
 * All variants do the same job: {@code float64} block &rarr; {@code int16}
 * block, 64<sup>3</sup> elements, each allocating its own output. They differ
 * in exactly one factor at a time, so consecutive pairs isolate a cost:
 * <table>
 * <tr><th>pair</th><th>isolates</th></tr>
 * <tr><td>{@link #a_checkedCodec} vs {@link #b_uncheckedViaReader}</td>
 *     <td>the NaN / infinity / range checks</td></tr>
 * <tr><td>{@link #b_uncheckedViaReader} vs {@link #c_uncheckedViaReaderNoRound}</td>
 *     <td>{@code Math.rint}</td></tr>
 * <tr><td>{@link #b_uncheckedViaReader} vs {@link #d_uncheckedDirect}</td>
 *     <td>the {@code DoubleReader} interface indirection</td></tr>
 * <tr><td>{@link #d_uncheckedDirect} vs {@link #e_uncheckedDirectToInt}</td>
 *     <td>{@code d2l} vs {@code d2i} narrowing</td></tr>
 * </table>
 * <p>
 * Reference points from the current benchmark run: the real codec measures
 * ~378 &micro;s and allocate-and-copy of an {@code int16} block ~48 &micro;s. If
 * the unchecked variants collapse toward the baseline, the checks (and the
 * vectorization they prevent) are the whole story. If they stay near 378, they
 * are not, and the cost is somewhere none of these factors covers.
 * <p>
 * The fill is deliberately in {@code [0, 100)} so that no variant ever takes an
 * out-of-range branch &mdash; the checked variant pays only for <em>testing</em>
 * the conditions, never for handling them. That is the fair comparison: it
 * isolates the cost the checks impose on the hot path.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class CastValueVectorizationProbe {

	@Param(value = {"64"})
	protected int blockDim;

	private static final int NUM_DIMENSIONS = 3;

	private static final DataType SOURCE_TYPE = DataType.FLOAT64;
	private static final DataType TARGET_TYPE = DataType.INT16;

	private int[] blockSize;
	private long[] gridPosition;
	private int numElements;

	private DataBlock<Object> sourceBlock;
	private double[] sourceData;

	private CastValueCodec<Object, Object> codec;
	private DoubleReader reader;

	public static void main(final String... args) throws RunnerException {

		final Options options = new OptionsBuilder()
				.include(CastValueVectorizationProbe.class.getSimpleName() + "\\.")
				.build();

		new Runner(options).run();
	}

	@SuppressWarnings("unchecked")
	@Setup(Level.Trial)
	public void setup() {

		blockSize = new int[NUM_DIMENSIONS];
		Arrays.fill(blockSize, blockDim);
		gridPosition = new long[NUM_DIMENSIONS];
		numElements = DataBlock.getNumElements(blockSize);

		sourceBlock = (DataBlock<Object>)SOURCE_TYPE.createDataBlock(blockSize, gridPosition);
		sourceData = (double[])sourceBlock.getData();

		final Random random = new Random(7777);
		for (int i = 0; i < sourceData.length; i++)
			sourceData[i] = random.nextDouble() * 100.0;

		codec = new CastValueCodec<>(SOURCE_TYPE, TARGET_TYPE, Rounding.NEAREST_EVEN, OutOfRange.CLAMP);
		reader = BlockElementAccess.reader(SOURCE_TYPE);
	}

	private DataBlock<?> newTargetBlock() {

		return TARGET_TYPE.createDataBlock(blockSize, gridPosition, numElements);
	}

	/** Control: the real codec, checks and all. */
	@Benchmark
	public void a_checkedCodec(final Blackhole hole) {

		hole.consume(codec.encode(sourceBlock));
	}

	/** Drop the checks; keep the reader indirection and the rounding. */
	@Benchmark
	public void b_uncheckedViaReader(final Blackhole hole) {

		final DataBlock<?> out = newTargetBlock();
		final short[] dst = (short[])out.getData();
		final Object src = sourceData;

		for (int i = 0; i < numElements; i++)
			dst[i] = (short)(long)Math.rint(reader.get(src, i));

		hole.consume(out);
	}

	/** Also drop the rounding. */
	@Benchmark
	public void c_uncheckedViaReaderNoRound(final Blackhole hole) {

		final DataBlock<?> out = newTargetBlock();
		final short[] dst = (short[])out.getData();
		final Object src = sourceData;

		for (int i = 0; i < numElements; i++)
			dst[i] = (short)(long)reader.get(src, i);

		hole.consume(out);
	}

	/** Keep the rounding, drop the reader: read {@code double[]} directly. */
	@Benchmark
	public void d_uncheckedDirect(final Blackhole hole) {

		final DataBlock<?> out = newTargetBlock();
		final short[] dst = (short[])out.getData();
		final double[] src = sourceData;

		for (int i = 0; i < numElements; i++)
			dst[i] = (short)(long)Math.rint(src[i]);

		hole.consume(out);
	}

	/**
	 * As {@link #d_uncheckedDirect}, but narrowing via {@code int} rather than
	 * {@code long}. Java's {@code d2l} carries saturation semantics that are
	 * more expensive than {@code d2i} on x86; once a value is known to be in
	 * range, {@code (int)} is exact for any target of 32 bits or fewer.
	 */
	@Benchmark
	public void e_uncheckedDirectToInt(final Blackhole hole) {

		final DataBlock<?> out = newTargetBlock();
		final short[] dst = (short[])out.getData();
		final double[] src = sourceData;

		for (int i = 0; i < numElements; i++)
			dst[i] = (short)(int)Math.rint(src[i]);

		hole.consume(out);
	}

	/** Allocation and bandwidth floor for an {@code int16} block. */
	@Benchmark
	public void f_baselineAllocate(final Blackhole hole) {

		final DataBlock<?> out = newTargetBlock();
		final short[] dst = (short[])out.getData();

		for (int i = 0; i < numElements; i++)
			dst[i] = (short)i;

		hole.consume(out);
	}

}
