package org.janelia.saalfeldlab.n5.benchmarks;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.Lz4Compression;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.XzCompression;
import org.janelia.saalfeldlab.n5.shard.BlockReadDataShard;
import org.janelia.saalfeldlab.n5.shard.Shard;
import org.janelia.saalfeldlab.n5.util.Position;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.google.gson.GsonBuilder;

@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 100, timeUnit = TimeUnit.MICROSECONDS)
@Measurement(iterations = 50, time = 100, timeUnit = TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class N5ShardModifyBenchmarks {

	public static final String GZIP_COMPRESSION = "gzip";
	public static final String RAW_COMPRESSION = "raw";
	public static final String LZ4_COMPRESSION = "lz4";
	public static final String XZ_COMPRESSION = "xz";

	Random random = new Random(7777);

	final String dset = "";

	N5Writer n5;
	DatasetAttributes dsetAttrs;
	DataBlock<?>[] blocks;
	DataBlock<?>[] blocksToAdd;
	ArrayList<long[]> blocksToDelete;

	@Param( value = { "int32" } )
	protected String dataType;

	@Param( value = { "256" } )
	protected int blockDim;

	@Param( value = { "36" } )
	protected int totalBlocksPerShard;

	@Param( value = { "12" } )
	protected int initialBlocksPerShard;

	@Param( value = { "4" } )
	protected int blocksToModify;

	@Param( value = { "raw", "gzip" } )
	protected String compressionString;
	
	int numBlocks;

	public static void main( String[] args ) throws RunnerException {

		final Options options = new OptionsBuilder().include( N5ShardModifyBenchmarks.class.getSimpleName() + "\\." ).build();
		new Runner(options).run();
	}

	@TearDown(Level.Trial)
	public void teardown() {
		File d = new File(n5.getURI());
		n5.remove();
		n5.close();
		d.delete();
	}

	@SuppressWarnings("unchecked")
	@Setup(Level.Trial)
	public <T> void setup() {

		File tmpDir;
		try {
			tmpDir = Files.createTempDirectory("n5-shardWriteBenchmark-").toFile();
			FileSystemKeyValueAccess kva = new FileSystemKeyValueAccess(FileSystems.getDefault());
			n5 = new N5KeyValueWriter(kva, tmpDir.getAbsolutePath(), new GsonBuilder(), true);

			final int[] blockSize = new int[]{blockDim};
			final int[] shardSize = new int[] {blockDim * totalBlocksPerShard };
			final long[] dims = new long[]{ blockDim * totalBlocksPerShard };

			final DataType dtype = DataType.fromString(dataType);
			dsetAttrs = new DatasetAttributes(dims, shardSize, blockSize, dtype, getCompression(compressionString));
			n5.createDataset("", dsetAttrs);

			long[] p = new long[1];
			blocks = new DataBlock[initialBlocksPerShard];
			fillBlocks(blocks, dtype, blockSize, p);

			n5.writeBlocks(dset, dsetAttrs, (DataBlock<T>[])blocks);

			blocksToAdd = new DataBlock[blocksToModify];
			fillBlocks(blocksToAdd, dtype, blockSize, p);

			blocksToDelete = new ArrayList<>();
			IntStream.range(0, blocksToModify).forEach(i -> {
				blocksToDelete.add(new long[]{i});
			});

		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	private void fillBlocks(DataBlock<?>[] blocks, DataType dtype, int[] blockSize, long[] p) {

		System.out.println(blocks);
		for (int i = 0; i < blocks.length; i++) {
			final DataBlock<?> blk = dtype.createDataBlock(blockSize, p);
			System.out.println(blk);
			fillBlock(dtype, blk);
			blocks[i] = blk;
			p[0]++;
		}
	}

	@Benchmark
	public void addBlocksSingleBenchmark() throws IOException {
		for (int i = 0; i < blocksToModify; i++) {
			n5.writeBlock(dset, dsetAttrs, blocksToAdd[i]);
		}
	}

	@SuppressWarnings("unchecked")
	@Benchmark
	public <T> void addBlocksBatchBenchmark() throws IOException {
		n5.writeBlocks(dset, dsetAttrs,(DataBlock<T>[]) blocksToAdd);
	}

	@Benchmark
	public void removeBlocksSingleBenchmark() throws IOException {

		blocksToDelete.forEach(p -> {
			n5.deleteBlock(dset, p);
		});
	}

	@Benchmark
	public <T> void removeBlocksBatchBenchmark() throws IOException {

		// Batch delete is not currently an API method for N5Writers,
		// this is how it would be implemented

		/* Group blocks by shard index */
		final Map<Position, List<long[]>> shardBlockMap = dsetAttrs.groupBlockPositions(blocksToDelete);
		for (final Entry<Position, List<long[]>> e : shardBlockMap.entrySet()) {

			final long[] shardPosition = e.getKey().get();
			final Shard<T> currentShard = n5.readShard(dset, dsetAttrs, shardPosition);
			final BlockReadDataShard<T> newShard;
			if (currentShard != null) {
				newShard = BlockReadDataShard.fromShard(currentShard);
			} else {
				newShard = new BlockReadDataShard<>(dsetAttrs, shardPosition);
			}

			for (long[] p : e.getValue())
				newShard.removeBlock(p);

			n5.writeShard(dset, dsetAttrs, newShard);
		}
	}

	private void fillBlock(DataType dtype, DataBlock<?> blk) {

		switch (dtype) {
		case INT32:
			fill((int[])blk.getData());
			break;
		case FLOAT32:
			fill((float[])blk.getData());
			break;
		case FLOAT64:
			fill((double[])blk.getData());
			break;
		case INT16:
			fill((short[])blk.getData());
			break;
		case INT64:
			fill((long[])blk.getData());
			break;
		case INT8:
			fill((byte[])blk.getData());
			break;
		case UINT16:
			fill((short[])blk.getData());
			break;
		case UINT32:
			fill((int[])blk.getData());
			break;
		case UINT64:
			fill((long[])blk.getData());
			break;
		case UINT8:
			fill((byte[])blk.getData());
			break;
		default:
			break;
		}
	}

	private void fill(short[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = (short)random.nextInt();
	}

	private void fill(int[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = random.nextInt();
	}

	private void fill(long[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = random.nextLong();
	}

	private void fill(float[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = random.nextFloat();
	}

	private void fill(double[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = random.nextDouble();
	}

	private void fill(byte[] arr) {
		random.nextBytes(arr);
	}

	private static Compression getCompression(final String compressionArg) {

		switch (compressionArg) {
		case GZIP_COMPRESSION:
			return new GzipCompression();
		case LZ4_COMPRESSION:
			return new Lz4Compression();
		case XZ_COMPRESSION:
			return new XzCompression();
		case RAW_COMPRESSION:
			return new RawCompression();
		default:
			throw new IllegalArgumentException("No compressor matches: " + compressionArg);
		}
	}
}
