package org.janelia.saalfeldlab.n5.kva;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
		name = "FsLockingValidation",
		mixinStandardHelpOptions = true,
		description = "Two-process test for FileLock race conditions.")
public class FsLockingValidation implements Callable<Integer> {

	@Option(names = {"--file", "-f"},
			required = true,
			description = "Path of the file used for the locking test.")
	String file;

	@Option(names = {"--data-size"},
			description = "Bytes written per repeat (default: ${DEFAULT-VALUE}).")
	int dataSize = 65_536;

	@Option(names = {"--num-repeats"},
			description = "Number of write/read cycles (default: ${DEFAULT-VALUE}).")
	int numRepeats = 1000;

	// Amount of sleep time after getting file size, but before attempting to actually read
	@Option(names = {"--sleep", "-s"},
			description = "ms writer sleeps after TRUNCATE_EXISTING and before lock() (default: ${DEFAULT-VALUE}).")
	long sleepMs = 0;

	static Random random = new Random();

	static final int id = random.nextInt(9999);


	public static void main(String[] args) {
		System.exit(new CommandLine(new FsLockingValidation()).execute(args));
	}

	@Override
	public Integer call() throws Exception {

		final FileSystemKeyValueAccess kva = new FileSystemKeyValueAccess();

		// Seed the file so it exists and has the expected size before any loop iteration.
		final byte[] seed = new byte[dataSize];
		Arrays.fill(seed, (byte) 0x11);
		kva.write(file, ReadData.from(seed));

		// Size of the "index" slice read from the end of the file, analogous to
		// RawShardCodec.decode() reading the shard index at indexOffset = requireLength() - indexBlockSizeInBytes.
		final long indexSliceSize = Math.max(1, dataSize / 8);

		int errors = 0;
		for (int i = 0; i < numRepeats; i++) {

			try {
				ReadData modifiedReadData = null;
				try (VolatileReadData vrd = kva.createReadData(file)) {

					// 1. slice the vrd for a subset near the end — mirrors the index read in
					//    RawShardCodec.decode(). requireLength() is where Files.size() is called
					//    and where a negative offset would be computed if the file was truncated
					//    by a concurrent writer before its exclusive lock was acquired.
					final long totalLength = vrd.requireLength();
					final long sliceOffset = totalLength - indexSliceSize;

					Thread.sleep(sleepMs);
					vrd.slice(sliceOffset, indexSliceSize);

					// 2. create new ReadData of dataSize, as if we merged the existing
					//    shard with new blocks and are ready to write the result.
					final byte[] newData = new byte[dataSize];
					Arrays.fill(newData, (byte) (i & 0xFF));

					// 3. set modifiedReadData
					modifiedReadData = ReadData.from(newData);
					modifiedReadData.materialize();
				}

				kva.write(file, modifiedReadData);
			} catch (Exception e) {
				errors++;
				System.err.printf("[id %d] ERROR at repeat %d: %s%n",
						id, i, e.getMessage());
			}

			if (i % 1000 == 0)
				System.out.printf("[id %d] repeat %d / %d (%d errors so far)%n",
						id, i, numRepeats, errors);
		}

		System.out.printf("[id %d] done. %d errors / %d repeats%n",
				id, errors, numRepeats);

		return errors > 0 ? 1 : 0;
	}

}
