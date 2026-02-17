package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.readdata.LazyRead;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

import static org.janelia.saalfeldlab.n5.FileKeyLockManager.FILE_LOCK_MANAGER;

public class FsIoPolicy {

    static final IoPolicy atomicWithFallback = IoPolicy.withFallback(new Atomic(), new Unsafe());

    private static boolean validBounds(long channelSize, long offset, long length) {

        if (offset < 0)
            return false;
        else if (channelSize > 0 && offset >= channelSize) // offset == 0 and channelSize == 0 is okay
            return false;
        else if (length >= 0 && offset + length > channelSize)
            return false;

        return true;
    }

    public static class Unsafe implements IoPolicy {
        @Override
        public void write(String key, ReadData readData) throws IOException {
            final Path path = Paths.get(key);
            Files.copy(readData.inputStream(), path, StandardCopyOption.REPLACE_EXISTING);
        }

        @Override
        public VolatileReadData read(final String key) throws IOException {
            final Path path = Paths.get(key);
            FileLazyRead fileLazyRead = new FileLazyRead(path, false);
            return VolatileReadData.from(fileLazyRead);
        }

        @Override
        public void delete(final String key) throws IOException {
            final Path path = Paths.get(key);
            Files.deleteIfExists(path);
        }
    }

    public static class Atomic implements IoPolicy {
        @Override
        public void write(String key, ReadData readData) throws IOException {
            final Path path = Paths.get(key);
            try (LockedFileChannel channel = FILE_LOCK_MANAGER.lockForWriting(path)) {
                readData.writeTo(channel.newOutputStream());
            }
        }

        @Override
        public VolatileReadData read(String key) throws IOException {
            final Path path = Paths.get(key);
            FileLazyRead fileLazyRead = new FileLazyRead(path, true);
            return VolatileReadData.from(fileLazyRead);
        }

        @Override
        public void delete(final String key) throws IOException {
            final Path path = Paths.get(key);
            try (LockedFileChannel ignore = FILE_LOCK_MANAGER.lockForWriting(path)) {
                Files.delete(path);
            }
        }
    }

    static class FileLazyRead implements LazyRead {

        private static final Closeable NO_OP = () -> { };

        private final Path path;
        private Closeable lock;

        FileLazyRead(final Path path) throws IOException {
            this(path, true);
        }

        FileLazyRead(final Path path, final boolean requireLock ) throws IOException {
            this.path = path;
            if (requireLock)
                lock = FILE_LOCK_MANAGER.lockForReading(path);
            else
                lock = NO_OP;
        }

        @Override
        public long size() throws N5Exception.N5IOException {

            if (lock == null) {
                throw new N5Exception.N5IOException("FileLazyRead is already closed.");
            }
            return FileSystemKeyValueAccess.size(path);
        }

        @Override
        public ReadData materialize(final long offset, final long length) {

            if (lock == null) {
                throw new N5Exception.N5IOException("FileLazyRead is already closed.");
            }

            try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {

                channel.position(offset);

                final long channelSize = channel.size();
                if (!validBounds(channelSize, offset, length)) {
                    throw new IndexOutOfBoundsException();
                }

                final long size = length < 0 ? (channelSize - offset) : length;
                if (size > Integer.MAX_VALUE) {
                    throw new IndexOutOfBoundsException("Attempt to materialize too large data");
                }

                final byte[] data = new byte[(int) size];
                final ByteBuffer buf = ByteBuffer.wrap(data);
                channel.read(buf);
                return ReadData.from(data);

            } catch (final NoSuchFileException e) {
                throw new N5Exception.N5NoSuchKeyException("No such file", e);
            } catch (IOException | UncheckedIOException e) {
                throw new N5Exception.N5IOException(e);
            }
        }

        @Override
        public void close() throws IOException {

            if (lock != null) {
                lock.close();
                lock = null;
            }
        }
    }
}
