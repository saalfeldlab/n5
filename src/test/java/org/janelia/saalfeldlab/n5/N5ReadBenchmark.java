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
package org.janelia.saalfeldlab.n5;

import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

@State( Scope.Thread )
@Fork( 1 )
public class N5ReadBenchmark {



	private static String tempN5PathName() {
		try {
			final File tmpFile = Files.createTempDirectory("n5-test-").toFile();
			tmpFile.deleteOnExit();
			return tmpFile.getCanonicalPath();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static final String basePath = tempN5PathName();
	private static final String datasetName = "/test/group/dataset";
	private static final long[] dimensions = new long[]{640, 640, 640};
	private static final int[] blockSize = new int[]{64, 64, 64};

	private static byte[] byteBlock;
	private static short[] shortBlock;
	private static int[] intBlock;
	private static long[] longBlock;
	private static float[] floatBlock;
	private static double[] doubleBlock;

	private static void createData() {
		final Random rnd = new Random();
		final int blockNumElements = DataBlock.getNumElements(blockSize);
		byteBlock = new byte[blockNumElements];
		shortBlock = new short[blockNumElements];
		intBlock = new int[blockNumElements];
		longBlock = new long[blockNumElements];
		floatBlock = new float[blockNumElements];
		doubleBlock = new double[blockNumElements];
		rnd.nextBytes(byteBlock);
		for (int i = 0; i < blockNumElements; ++i) {
			shortBlock[i] = (short)rnd.nextInt();
			intBlock[i] = rnd.nextInt();
			longBlock[i] = rnd.nextLong();
			floatBlock[i] = Float.intBitsToFloat(rnd.nextInt());
			doubleBlock[i] = Double.longBitsToDouble(rnd.nextLong());
		}
	}

	private static N5Writer createTempN5Writer() {
		return new N5FSWriter(basePath, new GsonBuilder());
	}

	private static N5Reader n5;
	private static DatasetAttributes attributes;

	@Setup(Level.Trial)
	public void setup() {
		createData();
		System.out.println("basePath = " + basePath);

		try (final N5Writer n5 = createTempN5Writer()) {
//			final Compression compression = new Lz4Compression();
			final Compression compression = new RawCompression();
			n5.createDataset(datasetName, dimensions, blockSize, DataType.FLOAT64, compression);
			final DatasetAttributes attributes = n5.getDatasetAttributes(datasetName);
			final DoubleArrayDataBlock dataBlock = new DoubleArrayDataBlock(blockSize, new long[] {0, 0, 0}, doubleBlock);
			n5.writeBlock(datasetName, attributes, dataBlock);
		}

		n5 = new N5FSReader(basePath, new GsonBuilder());
		attributes = n5.getDatasetAttributes(datasetName);
	}

//	@TearDown(Level.Trial)
//	public void teardown() {
//	}

	@Benchmark
	@BenchmarkMode( Mode.AverageTime )
	@OutputTimeUnit( TimeUnit.MILLISECONDS )
	public void bench() {
		n5.readBlock(datasetName, attributes, 0, 0, 0);
	}

	public static void main( final String... args ) throws RunnerException, IOException
	{
		final Options opt = new OptionsBuilder()
				.include( N5ReadBenchmark.class.getSimpleName() )
				.warmupIterations( 8 )
				.measurementIterations( 8 )
				.warmupTime( TimeValue.milliseconds( 500 ) )
				.measurementTime( TimeValue.milliseconds( 500 ) )
				.build();
		new Runner( opt ).run();
	}
}
