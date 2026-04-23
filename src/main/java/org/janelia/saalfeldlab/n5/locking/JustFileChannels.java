package org.janelia.saalfeldlab.n5.locking;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class JustFileChannels {

	// Idea:
	//
	// Writers write random valid files.
	//
	// Readers verify that a file is valid.
	// Verifying that a file is valid should take multiple reads, to make it
	// similar to chunk reading.
	//
	// Valid file has an "index" at the end.
	// The index is int[2*N] where N is predefined. The index comprises
	// consecutive pairs (offset, value) where `offset` is a byte offset in the
	// file and `value` is the in value that should be there.
	//
	// Writer creates int[random_length + 2*N] and fills the first
	// random_length ints with random data. It then draws N random indices into
	// that data, looks up those values in the data, and creates an index in
	// the final 2*N ints.
	//
	// To verify, the reader
	// 1. gets the channel size
	// 2. reads the index
	// 3. reads N 4-byte slices to verify that the file has the expected values
	//    at the expected indices.
	//

	static final int minDataSize = 1024;
	static final int maxDataSize = 1024 * 1024;
	static final int indexPairs = 100;

	static void write(String path, final Random random) {
		try {

			// NB: not creating any parent directories for now

			final Path p = Paths.get(path);
			final FileChannel channel = FileChannel.open(p, READ, WRITE, CREATE);
			final FileLock lock = channel.lock(0, Long.MAX_VALUE, false);
			channel.truncate(0);

			final int n = minDataSize + random.nextInt(maxDataSize - minDataSize);
			final int[] content = new int[n + 2 * indexPairs];
			for (int i = 0; i < n; i++) {
				content[i] = random.nextInt();
			}
			for (int i = n; i < n + 2 * indexPairs; i+=2) {
				final int offset = random.nextInt(n);
				content[i] = offset;
				content[i+1] = content[offset];
			}

			final int capacity = 4 * content.length;
			ByteBuffer buffer = ByteBuffer.allocate(capacity);
			buffer.asIntBuffer().put(content);
			if (channel.write(buffer) != capacity)
				throw new RuntimeException("write failed");

			lock.release();
			channel.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// throws RuntimeException if file is not valid
	static void verify(String path) {
		try {
			final Path p = Paths.get(path);
			final FileChannel channel = FileChannel.open(p, READ);
			final FileLock lock = channel.lock(0, Long.MAX_VALUE, true);

			final long size = channel.size();

			final int[] index = new int[2 * indexPairs];
			ByteBuffer buffer = ByteBuffer.allocate(index.length * 4);
			channel.read(buffer, size - 4 * index.length);
			buffer.position(0);
			buffer.asIntBuffer().get(index);

			for (int i = 0; i < indexPairs; i++) {
				final int offset = index[2 * i];
				final int expected = index[2 * i + 1];
				buffer = ByteBuffer.allocate(4);
				channel.read(buffer, offset * 4);
				buffer.position(0);
				final int actual = buffer.asIntBuffer().get(0);
				if (actual != expected)
					throw new RuntimeException("verify failed");
			}

			lock.release();
			channel.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) throws InterruptedException {
		
		// Repeatedly calls write, then verify using the file at path.
		// Sleeps for sleepTime(default=0) ms in between the write and verify calls.
		final Random random = new Random();
		final String path = args[0];
		final int N = Integer.parseInt(args[1]);

		long sleepTime = 0;
		if( args.length > 2)
			sleepTime = Long.parseLong(args[2]);
		
		for( int i = 0; i < N; i++ ) {
			write(path, random);
			Thread.sleep(sleepTime);
			verify(path);
		}
	}
}