package org.janelia.saalfeldlab.n5.benchmarks;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;

import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@Warmup(iterations = 10, time = 100, timeUnit = TimeUnit.MICROSECONDS)
@Measurement(iterations = 100, time = 100, timeUnit = TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class ReadDataBenchmarksKvaReadData extends ReadDataBenchmarks {

	public static void main(String... args) throws RunnerException {

		final Options options = new OptionsBuilder().include(ReadDataBenchmarksKvaReadData.class.getSimpleName() + "\\.")
				.build();

		new Runner(options).run();
	}

	public ReadData read() throws IOException {

		return ((FileSystemKeyValueAccess)kva).createReadData(getPath().toString());
	}

}