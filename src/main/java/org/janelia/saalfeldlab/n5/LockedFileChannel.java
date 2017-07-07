package org.janelia.saalfeldlab.n5;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public abstract class LockedFileChannel implements Closeable {

	private final FileChannel channel;

	LockedFileChannel(final Path path, final boolean readOnly) throws IOException {

		FileChannel channel = null;
		for (boolean waiting = true; waiting;) {
			waiting = false;
			try {
				final OpenOption[] options = readOnly ? new OpenOption[]{StandardOpenOption.READ} : new OpenOption[]{StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};
				channel = FileChannel.open(path, options);
				channel.lock(0L, Long.MAX_VALUE, readOnly);
			} catch (final OverlappingFileLockException e) {
				channel.close();
				waiting = true;
				try {
					Thread.sleep(100);
				} catch (final InterruptedException f) {
					channel = null;
					waiting = false;
					f.printStackTrace(System.err);
				}
			}
		}
		this.channel = channel;
	}

	public FileChannel getFileChannel() {

		return channel;
	}

	@Override
	public void close() throws IOException {

		channel.close();
	}

	public static final class ReadLockedFileChannel extends LockedFileChannel {

		public ReadLockedFileChannel(final Path path) throws IOException {

			super(path, true);
		}
	}

	public static final class WriteLockedFileChannel extends LockedFileChannel {

		public WriteLockedFileChannel(final Path path) throws IOException {

			super(path, false);
		}
	}
}