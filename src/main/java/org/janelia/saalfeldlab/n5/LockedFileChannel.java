package org.janelia.saalfeldlab.n5;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

/**
 * LockedFileChannel implementation for both read and write operations.
 * <p>
 * When closing this {@code LockedFileChannel}, {@code releaseLock} is called,
 * but the {@code channel} is not closed. The channel may be shared among
 * multiple {@code LockedFileChannel} for concurrent readers. {@code
 * releaseLock} should take care of closing the channel when the last reader (or
 * the only writer) is closed.
 */
class LockedFileChannel implements Closeable {

	// TODO: Consider splitting LockedFileChannel into read-only and write-only part.
	// TODO: Consider removing LockedFileChannel and having
	//       FileKeyLockManager.acquireRead() and FileKeyLockManager.acquireWrite()
	//       return appropriately wrapped SeekableByteChannel.

	private final FileChannel channel;
	private ReleaseLock releaseLock;

	@FunctionalInterface
	public interface ReleaseLock {

		void release() throws IOException;
	}

	LockedFileChannel(final FileChannel channel, final ReleaseLock releaseLock) {

		this.channel = channel;
		this.releaseLock = releaseLock;
	}

	/**
	 * Returns the size of this channel's file.
	 * <p>
	 * See {@link FileChannel#size()}.
	 */
	public long size() throws IOException {
		return channel.size();
	}

	/**
	 * Reads a sequence of bytes from this channel into the given buffer,
	 * starting at the given file position.
	 * <p>
	 * See {@link FileChannel#read(ByteBuffer, long)}.
	 */
	public int read(final ByteBuffer dst, final long position) throws IOException {
		return channel.read(dst, position);
	}

	/**
	 * Return an {@link OutputStream} that writes into this channel.
	 * Closing the OutputStream will close this channel.
	 */
	public OutputStream asOutputStream() {
		return Channels.newOutputStream(new ClosingChannelWrapper());
	}

	private class ClosingChannelWrapper implements WritableByteChannel {

		@Override
		public int write(final ByteBuffer src) throws IOException {
			return channel.write(src);
		}

		@Override
		public boolean isOpen() {
			return channel.isOpen();
		}

		@Override
		public void close() throws IOException {
			channel.close();
			LockedFileChannel.this.close();
		}
	}

	@Override
	public void close() throws IOException {

		if (releaseLock != null) {
			releaseLock.release();
			releaseLock = null;
			// Mote that setting releaseLock=null here drops the (method)
			// reference to LockKeyState, which potentially allows clearing the
			// WeakReference<LockKeyState> that FileLockManager holds.
		}
	}
}
