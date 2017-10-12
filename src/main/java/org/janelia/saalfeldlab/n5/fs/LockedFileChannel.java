package org.janelia.saalfeldlab.n5.fs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class LockedFileChannel implements Closeable {

	private final FileChannel channel;

	public static LockedFileChannel openForReading(final Path path) throws IOException {

		return new LockedFileChannel(path, true);
	}

	public static LockedFileChannel openForWriting(final Path path) throws IOException {

		return new LockedFileChannel(path, false);
	}

	private LockedFileChannel(final Path path, final boolean readOnly) throws IOException {

		final OpenOption[] options = readOnly ? new OpenOption[]{StandardOpenOption.READ} : new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};
		channel = FileChannel.open(path, options);

		for (boolean waiting = true; waiting;) {
			waiting = false;
			try {
				channel.lock(0L, Long.MAX_VALUE, readOnly);
			} catch (final OverlappingFileLockException e) {
				waiting = true;
				try {
					Thread.sleep(100);
				} catch (final InterruptedException f) {
					waiting = false;
					f.printStackTrace(System.err);
				}
			}
		}
	}

	public FileChannel getFileChannel() {

		return channel;
	}

	@Override
	public void close() throws IOException {

		channel.close();
	}
}