package org.janelia.saalfeldlab.n5.benchmarks;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
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

@State(Scope.Benchmark)
@Warmup(iterations = 10, time = 100, timeUnit = TimeUnit.MICROSECONDS)
@Measurement(iterations = 100, time = 100, timeUnit = TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
public class ReadDataBenchmarks {

	@Param(value = { "10000000" })
	protected int objectSizeBytes;

	protected Path basePath;
	protected ArrayList<Path> tmpPaths;
	protected KeyValueAccess kva;
	protected Random random;

	public ReadDataBenchmarks() {}

	public static void main(String... args) throws RunnerException {

		final Options options = new OptionsBuilder().include(ReadDataBenchmarks.class.getSimpleName() + "\\.")
				.build();

		new Runner(options).run();
	}

	@Benchmark
	public void run(Blackhole hole) throws IOException {

		hole.consume(read().materialize());
	}

	public ReadData read() throws IOException {

		return kva.createReadData(getPath().toString());
	}

	protected Path getPath() {

		return basePath.resolve("tmp-" + objectSizeBytes);
	}

	@Setup(Level.Trial)
	public void setup() throws IOException {

		random = new Random();
		kva = new FileSystemKeyValueAccess(FileSystems.getDefault());

		basePath = Files.createTempDirectory("ReadDataBenchmark-");
		tmpPaths = new ArrayList<>();
		for (final int sz : sizes()) {
			Path p = basePath.resolve("tmp-"+sz);
			write(p, sz);
			tmpPaths.add(p);
		}
	}

	protected void write(Path path, int numBytes) {

		final byte[] data = new byte[numBytes];
		random.nextBytes(data);

		System.out.println(path.toAbsolutePath().toString());
		System.out.println(numBytes);
		try (final LockedChannel ch = kva.lockForWriting(path.toAbsolutePath().toString())) {
			final OutputStream os = ch.newOutputStream();
			os.write(data);
			os.flush();
			os.close();
		} catch (final IOException e) {
			e.printStackTrace();
		}
	}

	@TearDown(Level.Trial)
	public void teardown() {

		for ( Path p : tmpPaths ) {
			p.toFile().delete();
		}
		basePath.toFile().delete();
	}

	public int[] sizes() {

		try {
			final Param ann = ReadDataBenchmarks.class.getDeclaredField("objectSizeBytes").getAnnotation(Param.class);
			System.out.println(Arrays.toString(ann.value()));
			return Arrays.stream(ann.value()).mapToInt(Integer::parseInt).toArray();

		} catch (final NoSuchFieldException e) {
			e.printStackTrace();
		} catch (final SecurityException e) {
			e.printStackTrace();
		}

		return null;
	}

}
