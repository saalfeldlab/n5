package org.janelia.saalfeldlab.n5;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * LockedFileChannel implementation for both read and write operations.
 */
// TODO: This only has to be public because of a test in another package. Fix that
public class LockedFileChannel implements LockedChannel {

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

        truncateChannel();
        return Channels.newWriter(channel, StandardCharsets.UTF_8.name());
    }

    @Override
    public OutputStream newOutputStream() throws N5Exception.N5IOException {

        truncateChannel();
        return Channels.newOutputStream(channel);
    }

	// TODO: This only has to be public because of a test in another package. Fix that
    public FileChannel getFileChannel() {

        return channel;
    }

    private void truncateChannel() throws N5Exception.N5IOException {

        try {
            channel.truncate(0);
        } catch (final IOException e) {
            throw new N5Exception.N5IOException("Failed to truncate channel", e);
        }
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
