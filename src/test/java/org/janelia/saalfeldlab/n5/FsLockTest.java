package org.janelia.saalfeldlab.n5;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FsLockTest {

    private static final FileKeyLockManager LOCK_MANAGER = FileKeyLockManager.FILE_LOCK_MANAGER;

    private static String tempPathName() {

        try {
            final File tmpFile = Files.createTempDirectory("fs-key-lock-test-").toFile();
            tmpFile.delete();
            tmpFile.mkdir();
            tmpFile.deleteOnExit();
            return tmpFile.getCanonicalPath();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testReadLock() throws IOException {

        final Path path = Paths.get(tempPathName(), "lock");
        path.toFile().createNewFile();
        assertTrue("File Created", path.toFile().exists());
        LockedChannel lock = LOCK_MANAGER.lockForReading(path);
        lock.close();
        lock = LOCK_MANAGER.lockForReading(path);

        final ExecutorService exec = Executors.newSingleThreadExecutor();
        final Future<Void> future = exec.submit(() -> {
            LOCK_MANAGER.lockForWriting(path).close();
            return null;
        });

        try {
            System.out.println("Trying to acquire locked readable channel...");
            System.out.println(future.get(3, TimeUnit.SECONDS));
            fail("Lock broken!");
        } catch (final TimeoutException e) {
            System.out.println("Lock held!");
            future.cancel(true);
        } catch (final InterruptedException | ExecutionException e) {
            future.cancel(true);
            System.out.println("Test was interrupted!");
        } finally {
            lock.close();
            Files.delete(path);
        }

        exec.shutdownNow();
    }

    @Test
    public void testWriteLock() throws IOException {

        final Path path = Paths.get(tempPathName(), "lock");
        final LockedChannel lock = LOCK_MANAGER.lockForWriting(path);
        System.out.println("locked");

        final ExecutorService exec = Executors.newSingleThreadExecutor();
        final Future<Void> future = exec.submit(() -> {
            LOCK_MANAGER.lockForReading(path).close();
            return null;
        });

        try {
            System.out.println("Trying to acquire locked writable channel...");
            System.out.println(future.get(3, TimeUnit.SECONDS));
            fail("Lock broken!");
        } catch (final TimeoutException e) {
            System.out.println("Lock held!");
            future.cancel(true);
        } catch (final InterruptedException | ExecutionException e) {
            future.cancel(true);
            System.out.println("Test was interrupted!");
        } finally {
            lock.close();
            Files.delete(path);
        }

        exec.shutdownNow();
    }

    @Test
    public void testFSLockRelease() throws IOException, ExecutionException, InterruptedException, TimeoutException {


        final Path path = Paths.get(tempPathName(), "lock");
        final ExecutorService exec = Executors.newFixedThreadPool(2);

        // first thread acquires the lock, waits for 200ms then should release it
        exec.submit(() -> {
            try {
                try(final LockedChannel lock = LOCK_MANAGER.lockForWriting(path)) {
                    lock.newReader();
                    Thread.sleep(200);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // second thread waits for the lock.
        // it should get it within a few seconds.
        final Future<Void> future = exec.submit(() -> {
            LOCK_MANAGER.lockForWriting(path).close();
            return null;
        });

        future.get(3, TimeUnit.SECONDS);
        Files.delete(path);
        exec.shutdownNow();
    }

    @Test
    public void testReadLockBehavior() throws IOException, InterruptedException, ExecutionException, TimeoutException {

        final Path path = Paths.get(tempPathName(), "read-lock");
        path.toFile().createNewFile();

        final ExecutorService exec = Executors.newFixedThreadPool(3);

        final AtomicBoolean v = new AtomicBoolean(false);

        // first thread acquires a read lock, waits for 200ms
        Future<?> f = exec.submit(() -> {
            try {
                try(final LockedChannel lock = LOCK_MANAGER.lockForReading(path)) {
                    lock.newReader();
                    Thread.sleep(200);

                    // ensure that the other thread updated the value
                    assertTrue(v.get());

                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // second thread gets a read lock
        // and should not be blocked
        // this thread updates the boolean
        exec.submit(() -> {
            try( final LockedChannel lock = LOCK_MANAGER.lockForReading(path)) {
                lock.newReader();
                v.set(true);
            }
            return null;
        });

        f.get(3, TimeUnit.SECONDS);
        exec.shutdownNow();
        Files.delete(path);
    }

    @Test
    public void testWriteLockBehavior() throws IOException, ExecutionException, InterruptedException, TimeoutException {


        final Path path = Paths.get(tempPathName(), "lock");
        final ExecutorService exec = Executors.newFixedThreadPool(2);

        // first thread acquires the lock, waits for 200ms then should release it
        exec.submit(() -> {
            try {
                try(final LockedChannel lock = LOCK_MANAGER.lockForWriting(path)) {
                    lock.newReader();
                    Thread.sleep(200);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // second thread waits for the lock.
        // it should get it within a few seconds.
        final Future<Void> future = exec.submit(() -> {
            LOCK_MANAGER.lockForWriting(path).close();
            return null;
        });

        future.get(3, TimeUnit.SECONDS);
        Files.delete(path);
        exec.shutdownNow();
    }
}
