package org.janelia.saalfeldlab.n5;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * LockedFileChannel implementation for both read and write operations.
 */
public class LockedFileChannel implements LockedChannel {

    private final KeyLockState state;
    private final FileChannel channel;

    public LockedFileChannel(final KeyLockState state, final FileChannel channel) {

        this.state = state;
        this.channel = channel;
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
        state.releaseLock();
    }
}
