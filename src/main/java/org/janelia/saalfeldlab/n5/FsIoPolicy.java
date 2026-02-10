package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;

import static org.janelia.saalfeldlab.n5.FileKeyLockManager.FILE_LOCK_MANAGER;

public class FsIoPolicy {

    static final IoPolicy atomicWithFallback = IoPolicy.withFallback(new Atomic(), new Unsafe());

    public static class Unsafe implements IoPolicy {
        @Override
        public void write(String key, ReadData readData) throws IOException {
            final Path path = Paths.get(key);
            Files.copy(readData.inputStream(), path, StandardCopyOption.REPLACE_EXISTING);
        }

        @Override
        public VolatileReadData read(final String key) throws IOException {
            final Path path = Paths.get(key);
            FileSystemKeyValueAccess.FileLazyRead fileLazyRead = new FileSystemKeyValueAccess.FileLazyRead(path, false);
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
            FileSystemKeyValueAccess.FileLazyRead fileLazyRead = new FileSystemKeyValueAccess.FileLazyRead(path, true);
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
}
