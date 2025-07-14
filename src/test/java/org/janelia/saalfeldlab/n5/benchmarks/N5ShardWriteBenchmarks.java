package org.janelia.saalfeldlab.n5.benchmarks;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.codec.RawBytes;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
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
import org.openjdk.jmh.infra.Blackhole;
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
public class N5ShardWriteBenchmarks {

	public static final String GZIP_COMPRESSION = "gzip";
	public static final String RAW_COMPRESSION = "raw";
	public static final String LZ4_COMPRESSION = "lz4";
	public static final String XZ_COMPRESSION = "xz";

	Random random = new Random(7777);

	final String writeGroup = "writeGroup";
	final String readGroup = "readGroup";

	N5Writer n5;
	DatasetAttributes dsetAttrs;
	ArrayList<DataBlock<?>> blocks;

	@Param( value = { "int32" } )
	protected String dataType;

	@Param( value = { "64" } )
	protected int blockDim;

	@Param( value = { "12" } )
	protected int blocksPerShard;

	@Param( value = { "5" } )
	protected int numShards;
	
	@Param( value = { "raw", "gzip" } )
	protected String compressionString;
	
	int numBlocks;

	public static void main( String[] args ) throws RunnerException {

		final Options options = new OptionsBuilder().include( N5ShardWriteBenchmarks.class.getSimpleName() + "\\." ).build();
		new Runner(options).run();
	}

	@TearDown(Level.Trial)
	public void teardown() {
		File d = new File(n5.getURI());
		n5.remove();
		d.delete();
	}

	@Setup(Level.Trial)
	public void setup() {

		File tmpDir;
		try {
			tmpDir = Files.createTempDirectory("n5-shardWriteBenchmark-").toFile();
			FileSystemKeyValueAccess kva = new FileSystemKeyValueAccess(FileSystems.getDefault());
			n5 = new N5KeyValueWriter(kva, tmpDir.getAbsolutePath(), new GsonBuilder(), true);

			int[] blockSize = new int[1];
			Arrays.fill(blockSize, blockDim);

			int[] shardSize = new int[1];
			Arrays.fill(shardSize, blockDim * blocksPerShard);

			long[] dims = new long[1];
			Arrays.fill(dims, blockDim);
			dims[0] = blockDim * blocksPerShard * numShards;

			numBlocks = blocksPerShard * numShards;

			DataType dtype = DataType.fromString(dataType);

			dsetAttrs = new DatasetAttributes(
					dims,
					shardSize,
					blockSize,
					dtype,
					new ShardingCodec(
							blockSize,
							new Codec[]{new N5BlockCodec(), BenchmarkUtils.getCompression(compressionString)},
							new DeterministicSizeCodec[]{new RawBytes(), new Crc32cChecksumCodec()},
							IndexLocation.END
					)
			);
			n5.createDataset("", dsetAttrs);

			blocks = new ArrayList<>();
			long[] p = new long[1];
			for (int i = 0; i < numBlocks; i++) {
				p[0] = i;

				DataBlock<?> blk = dtype.createDataBlock(blockSize, p);
				BenchmarkUtils.fillBlock(dtype, blk);
				blocks.add(blk);

				// write data into the read group
				n5.writeBlock(readGroup, dsetAttrs, blk);
			}

		} catch (final IOException e) {
			e.printStackTrace();
		}

	}

	@Benchmark
	public void writeSingleBlockBenchmark() throws IOException {

		blocks.forEach(blk -> {
			n5.writeBlock(writeGroup, dsetAttrs, blk);
		});
	}
	
	@SuppressWarnings("unchecked")
	@Benchmark
	public void writeMultiBlockBenchmark() throws IOException {

		n5.writeBlocks(writeGroup, dsetAttrs, blocks.toArray(new DataBlock[0]));
	}

	@Benchmark
	public void readBenchmark(Blackhole hole) throws IOException {

		final long[] p = new long[1];
		for (int i = 0; i < numBlocks; i++) {
			p[0] = i;
			hole.consume(n5.readBlock(readGroup, dsetAttrs, p));
		}
	}

}
