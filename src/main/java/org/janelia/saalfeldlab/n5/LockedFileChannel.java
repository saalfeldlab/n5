package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * LockedFileChannel implementation for both read and write operations.
 */
class LockedFileChannel implements LockedChannel {

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

	@Override
	public Reader newReader() throws N5Exception.N5IOException {

		return Channels.newReader(channel, StandardCharsets.UTF_8.name());
	}

	@Override
	public InputStream newInputStream() throws N5Exception.N5IOException {

		return Channels.newInputStream(channel);
	}

	@Override
	public Writer newWriter() throws N5Exception.N5IOException {

		return Channels.newWriter(channel, StandardCharsets.UTF_8.name());
	}

	@Override
	public OutputStream newOutputStream() throws N5Exception.N5IOException {

		return Channels.newOutputStream(channel);
	}

	@Override
	public void close() throws IOException {

		channel.close();
		if (releaseLock != null) {
			releaseLock.release();
			releaseLock = null;
			// Mote that setting releaseLock=null here drops the (method)
			// reference to LockKeyState, which potentially allows clearing the
			// WeakReference<LockKeyState> that FileLockManager holds.
		}
	}
}
