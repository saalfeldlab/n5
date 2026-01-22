package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Semaphore;

/**
 * Per-key state that tracks both thread locks and file locks.
 */
class KeyLockState {

	private final Path path;

	public KeyLockState(final Path path) {
		this.path = path;
	}

	/**
	 * The current system-level file lock (shared for writing, non-shared for
	 * reading). Is {@link ChannelLock#close closed} when the last {@link
	 * #releaseRead() Reader is released}, or the (one and only) {@link
	 * #releaseWrite() Writer is released}.
	 */
	private volatile ChannelLock channelLock;

	/**
	 * Multiple Readers coordinate via this mutex. {@code numReaders} may only
	 * be modified when {@code readerMutex} is held.
	 */
	private final Semaphore readerMutex = new Semaphore(1);
	private int numReaders = 0;

	/**
	 * This coordinates mutual exclusion between one writer and (the first of)
	 * multiple readers. {@code channelLock} may only be created or closed when
	 * {@code channelLockMutex} is held.
	 */
	private final Semaphore channelLockMutex = new Semaphore(1);

	LockedFileChannel acquireRead() throws IOException {

		try {
			readerMutex.acquire();
			try {
				if (numReaders == 0) {
					// We are the first Reader, and are responsible for creating the channelLock
					// (Other concurrent Readers will still be blocked in readerMutex.)

					// If a Writer is still open, this will block us until the Writer is closed.
					channelLockMutex.acquire();

					try {
						channelLock = ChannelLock.lock(path, false);
					} catch (IOException e) {
						// Something went wrong. Back off.
						channelLockMutex.release();
						throw e;
					}
				}

				// We have a READ ChannelLock.
				// Try to open a FileChannel.
				final FileChannel channel;
				try {
					channel = FileChannel.open(path, StandardOpenOption.READ);
				} catch (final IOException e) {
					// Something went wrong. Back off.
					if (numReaders == 0) {
						releaseChannelLock();
					}
					throw e;
				}

				// We have a FileChannel.
				// Create a LockedFileChannel that will releaseRead() when it is closed.
				++numReaders;
				return new LockedFileChannel(channel, this::releaseRead);
			} finally {
				readerMutex.release();
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	LockedFileChannel tryAcquireRead() {

		if(!readerMutex.tryAcquire()) {
			return null;
		}

		try {
			if (numReaders == 0) {
				// We are the first Reader, and are responsible for creating the channelLock
				// (Other concurrent Readers are still blocked in readerMutex.)

				// If a Writer is still open, this will fail
				if(!channelLockMutex.tryAcquire()) {
					return null;
				}

				try {
					channelLock = ChannelLock.tryLock(path, false);
				} catch (IOException e) {
					// Something went wrong. Back off.
					channelLockMutex.release();
					return null;
				}
			}

			// We have a READ ChannelLock.
			// Try to open a FileChannel.
			final FileChannel channel;
			try {
				channel = FileChannel.open(path, StandardOpenOption.READ);
			} catch (final IOException e) {
				// Something went wrong. Back off.
				if (numReaders == 0) {
					try {
						releaseChannelLock();
					} catch (IOException ignored) {
					}
				}
				return null;
			}

			// We have a FileChannel.
			// Create a LockedFileChannel that will releaseRead() when it is closed.
			++numReaders;
			return new LockedFileChannel(channel, this::releaseRead);
		} finally {
			readerMutex.release();
		}
	}

	void releaseRead() throws IOException {

		try {
			readerMutex.acquire();
			try {
				--numReaders;
				if (numReaders == 0) {
					// We were the last Reader, and are responsible for releasing the channelLock
					releaseChannelLock();
				}
			} finally {
				readerMutex.release();
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	private void releaseChannelLock() throws IOException {

		try {
			channelLock.close();
		} finally {
			channelLockMutex.release();
		}
	}

	LockedFileChannel acquireWrite() throws IOException {

		try {
			// If another Writer or Reader is still open, this will block until it is closed.
			channelLockMutex.acquire();

			try {
				channelLock = ChannelLock.lock(path, true);
			} catch (IOException e) {
				// Something went wrong. Back off.
				channelLockMutex.release();
				throw e;
			}

			// We have a WRITE ChannelLock.
			// Try to open a FileChannel.
			final FileChannel channel;
			try {
				channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
			} catch (IOException e) {
				// Something went wrong. Back off.
				try {
					channelLock.close();
				} finally {
					channelLockMutex.release();
				}
				throw e;
			}

			// We have a FileChannel.
			// Create a LockedFileChannel that will releaseWrite() when it is closed.
			return new LockedFileChannel(channel, this::releaseWrite);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	LockedFileChannel tryAcquireWrite() {

		if (!channelLockMutex.tryAcquire()) {
			return null;
		}

		try {
			channelLock = ChannelLock.tryLock(path, true);
		} catch (IOException e) {
			// Something went wrong. Back off.
			channelLockMutex.release();
			return null;
		}

		// We have a WRITE ChannelLock.
		// Try to open a FileChannel.
		final FileChannel channel;
		try {
			channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
		} catch (IOException e) {
			// Something went wrong. Back off.
			try {
				releaseChannelLock();
			} catch (IOException ignored) {
			}
			return null;
		}

		// We have a FileChannel.
		// Create a LockedFileChannel that will releaseWrite() when it is closed.
		return new LockedFileChannel(channel, this::releaseWrite);
	}

	void releaseWrite() throws IOException {
		releaseChannelLock();
	}
}
