package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Per-key state that tracks both thread locks and file locks.
 */
class KeyLockState {

    private final ReentrantReadWriteLock threadLock = new ReentrantReadWriteLock();
    private final Path path;

    private FileChannel fileChannel;
    private FileLock fileLock;
    private int readLockHolders = 0;
    private boolean writeLockHeld = false;

    public KeyLockState(final Path path) {
        this.path = path;
    }

    synchronized boolean isLocked() {
        return threadLock.isWriteLocked() || threadLock.getReadLockCount() != 0 || readLockHolders != 0 || writeLockHeld;
    }

    void lockForReading() throws IOException {
        threadLock.readLock().lock();
        fileReadLock();
    }

    private synchronized void fileReadLock() throws IOException {
        try {
            if (readLockHolders == 0 && !writeLockHeld) {
                acquireFileLock(true);
            }
            readLockHolders++;
        } catch (final IOException e) {
            threadLock.readLock().unlock();
            throw e;
        }
    }

    void lockForWriting() throws IOException {
        threadLock.writeLock().lock();
        fileWriteLock();
    }

    private synchronized void fileWriteLock() throws IOException {
        try {
            acquireFileLock(false);
            writeLockHeld = true;
        } catch (final IOException e) {
            threadLock.writeLock().unlock();
            throw e;
        }
    }

    boolean tryLockForReading() {
        return threadLock.readLock().tryLock() && tryFileReadLock();
    }

    private synchronized boolean tryFileReadLock() {

        if (readLockHolders == 0 && !writeLockHeld) {
            if (!tryAcquireFileLock(true)) {
                threadLock.readLock().unlock();
                return false;
            }
        }
        readLockHolders++;
        return true;
    }

    private boolean tryAcquireFileLock(final boolean shared) {

        try {
            openFileChannel(shared);
            fileLock = fileChannel.tryLock(0, Long.MAX_VALUE, shared);
            if (fileLock == null) {
                closeLockChannelQuietly();
                return false;
            }
            return true;
        } catch (final OverlappingFileLockException | IOException e) {
            closeLockChannelQuietly();
            return false;
        }
    }

    boolean tryLockForWriting() {
        return threadLock.writeLock().tryLock() && tryFileWriteLock();
    }

    private synchronized boolean tryFileWriteLock() {
        if (!tryAcquireFileLock(false)) {
            threadLock.writeLock().unlock();
            return false;
        }
        writeLockHeld = true;
        return true;
    }

    private void closeLockChannelQuietly() {

        if (fileChannel != null) {
            try {
                fileChannel.close();
            } catch (final IOException e) {
                /* ignore */
            }
            fileChannel = null;
        }
    }

    private void acquireFileLock(final boolean shared) throws IOException {

        openFileChannel(shared);

        while (true) {
            try {
                fileLock = fileChannel.lock(0, Long.MAX_VALUE, shared);
                return;
            } catch (final OverlappingFileLockException e) {
                try {
                    wait(100);
                } catch (final InterruptedException ie) {
                    closeLockChannelQuietly();
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for file lock", ie);
                }
            }
        }
    }

    /**
     * Opens a file channel; if not shared, then this may create the file and the parent directories as needed.
     *
     * @throws IOException if the channel cannot be opened
     */
    private void openFileChannel(final boolean shared) throws IOException {

        if (shared) {
            fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        } else {
            final Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        }
    }

    private void releaseFileLock() {

        if (fileLock != null) {
            try {
                fileLock.release();
            } catch (final IOException e) {
                /* ignore */
            }
            fileLock = null;
        }
        closeLockChannelQuietly();
        notifyAll();
    }

    void releaseLock() {

        final boolean isWrite = threadLock.isWriteLockedByCurrentThread();

        synchronized (this) {
            if (isWrite) {
                writeLockHeld = false;
                releaseFileLock();
            } else {
                readLockHolders--;
                if (readLockHolders == 0) {
                    releaseFileLock();
                }
            }
        }

        if (isWrite) {
            threadLock.writeLock().unlock();
        } else {
            threadLock.readLock().unlock();
        }
    }

    /**
     * Release file lock only, for cleanup when the LockedFileChannel was GC'd
     * without being closed. Does not release thread locks since those can only
     * be released by the owning thread.
     */
    synchronized void releaseFileLockForCleanup() {
        writeLockHeld = false;
        readLockHolders = 0;
        releaseFileLock();
    }

}
