package org.janelia.saalfeldlab.n5;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Holds a channel and system-level file lock (shared for writing, non-shared
 * for reading) and keeps it open until this {@code ChannelLock} is {@link
 * #close() closed}.
 */
class ChannelLock implements Closeable {

	private final FileChannel channel;
	private final FileLock lock;

	private ChannelLock(final FileChannel channel, final FileLock lock) {
		this.channel = channel;
		this.lock = lock;
	}

	public void close() throws IOException {

		// NB: We do not call lock.release() here, because it may throw an
		// exception if the channel is already closed. Instead, we just close
		// the channel. This will automatically release the lock. (And it is ok
		// to close an already closed channel.)

		channel.close();
	}

	FileChannel getChannel() {
		return channel;
	}

	/**
	 * Create a {@link FileChannel} on the given {@code path} and lock it with a
	 * system-level {@link FileLock}. If there is an existing overlapping file
	 * lock, this method will block until the existing lock is released and the
	 * channel could be locked (by us).
	 * <p>
	 * The {@code FileLock} is exclusive if the {@code path} is locked {@code
	 * forWriting}, and shared otherwise.
	 * <p>
	 * If the {@code path} is locked {@code forWriting} non-existing file and
	 * the parent directories are created as needed.
	 *
	 * @throws IOException if an error occurs while opening the channel, or if
	 * the calling thread is interrupted while waiting for the {@code FileLock}.
	 */
	static ChannelLock lock(final Path path, final boolean forWriting) throws IOException {

		final FileChannel channel = openFileChannel(path, forWriting);
		try {
			while (true) {
				try {
					final FileLock lock = channel.lock(0, Long.MAX_VALUE, !forWriting);
					return new ChannelLock(channel, lock);
				} catch (final OverlappingFileLockException e) {
					try {
						Thread.sleep(100);
					} catch (final InterruptedException ie) {
						Thread.currentThread().interrupt();
						throw new IOException("Interrupted while waiting for file lock", ie);
					}
				}
			}
		} catch (Exception e) {
			closeQuietly(channel);
			throw e;
		}
	}

	/**
	 * Create a {@link FileChannel} on the given {@code path} and try to lock it
	 * with a system-level {@link FileLock}. If the channel cannot be locked,
	 * {@code null} is returned.
	 * <p>
	 * The {@code FileLock} is exclusive if the {@code path} is locked {@code
	 * forWriting}, and shared otherwise.
	 * <p>
	 * If the {@code path} is locked {@code forWriting} non-existing file and
	 * the parent directories are created as needed.
	 *
	 * @throws IOException if an error occurs while opening the channel.
	 */
	static ChannelLock tryLock(final Path path, final boolean forWriting) throws IOException {

		FileChannel channel = null;
		try {
			channel = openFileChannel(path, forWriting);
			final FileLock lock = channel.tryLock(0, Long.MAX_VALUE, !forWriting);
			return lock == null ? null : new ChannelLock(channel, lock);
		} catch (Exception e) {
			closeQuietly(channel);
			throw e;
		}
	}

	/**
	 * Opens a file channel. If the channel is opened {@code forWriting},
	 * then this may create the file and the parent directories as needed.
	 *
	 * @throws IOException
	 * 		if the channel cannot be opened
	 */
	private static FileChannel openFileChannel(final Path path, final boolean forWriting) throws IOException {

		if (forWriting) {
			final Path parent = path.getParent();
			if (parent != null) {
				Files.createDirectories(parent);
			}
			return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
		} else {
			return FileChannel.open(path, StandardOpenOption.READ);
		}
	}

	private static void closeQuietly(final FileChannel fileChannel) {
		if (fileChannel != null) {
			try {
				fileChannel.close();
			} catch (final IOException ignored) {
			}
		}
	}
}
