/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.benchmarks;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
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
public class N5BlockWriteBenchmarks {

	Random random = new Random(7777);

	final String writeGroup = "writeGroup";
	final String readGroup = "readGroup";

	N5Writer n5;
	DatasetAttributes dsetAttrs;
	ArrayList<DataBlock<?>> blocks;

	@Param( value = { "int32" } )
	protected String dataType;

	@Param( value = { "3" } )
	protected int numDimensions;

	@Param( value = { "64" } )
	protected int blockDim;

	@Param( value = { "5" } )
	protected int numBlocks;

	public static void main( String[] args ) throws RunnerException {

		final Options options = new OptionsBuilder().include( N5BlockWriteBenchmarks.class.getSimpleName() + "\\." ).build();
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
			tmpDir = Files.createTempDirectory("n5-blockWriteBenchmark-").toFile();
			FileSystemKeyValueAccess kva = new FileSystemKeyValueAccess();
			n5 = new N5KeyValueWriter(kva, tmpDir.getAbsolutePath(), new GsonBuilder(), true);

			int[] blockSize = new int[numDimensions];
			Arrays.fill(blockSize, blockDim);

			long[] dims = new long[numDimensions];
			Arrays.fill(dims, blockDim);
			dims[0] = blockDim * numBlocks;

			DataType dtype = DataType.fromString(dataType);

			dsetAttrs = new DatasetAttributes(dims, blockSize, dtype, new GzipCompression());
			n5.createDataset("", dsetAttrs);

			blocks = new ArrayList<>();
			for (int i = 0; i < numBlocks; i++) {
				long[] p = new long[numDimensions];
				p[0] = i;

				DataBlock<?> blk = dtype.createDataBlock(blockSize, p);
				fillBlock(dtype, blk);
				blocks.add(blk);

				// write data into the read group
				n5.writeBlock(readGroup, dsetAttrs, blk);
			}

		} catch (final IOException e) {
			e.printStackTrace();
		}

	}

	@Benchmark
	public void writeBenchmark() throws IOException {

		blocks.forEach(blk -> {
			n5.writeBlock(writeGroup, dsetAttrs, blk);
		});
	}

	@Benchmark
	public void readBenchmark(Blackhole hole) throws IOException {

		final long[] p = new long[numDimensions];
		for (int i = 0; i < numBlocks; i++) {
			p[0] = i;
			hole.consume(n5.readBlock(readGroup, dsetAttrs, p));
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
		case OBJECT:
			break;
		case STRING:
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

}
