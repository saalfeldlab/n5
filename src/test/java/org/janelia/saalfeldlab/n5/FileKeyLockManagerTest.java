package org.janelia.saalfeldlab.n5;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.janelia.saalfeldlab.n5.FileKeyLockManager.FILE_LOCK_MANAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FileKeyLockManagerTest {

	private Path tempDir;

	@Before
	public void setUp() throws IOException {

		tempDir = Files.createTempDirectory("fklm-test");
	}

	@After
	public void tearDown() throws IOException {

		if (tempDir != null) {
			Files.walk(tempDir)
					.sorted((a, b) -> -a.compareTo(b))
					.forEach(p -> {
						try {
							Files.deleteIfExists(p);
						} catch (IOException e) {
							// ignore
						}
					});
		}
	}

	@Test
	public void testConcurrentReads() throws Exception {

		// create a test file
		final Path testFile = tempDir.resolve("test.txt");
		final byte[] testContent = "test content for concurrent reads".getBytes();
		Files.write(testFile, testContent);

		final int numReaders = 5;
		final ExecutorService executor = Executors.newFixedThreadPool(numReaders);

		final CountDownLatch startLatch = new CountDownLatch(1);
		final CountDownLatch readersReady = new CountDownLatch(numReaders);
		final CountDownLatch readersFinished = new CountDownLatch(numReaders);
		final AtomicInteger concurrentReaders = new AtomicInteger(0);
		final AtomicInteger maxConcurrentReaders = new AtomicInteger(0);
		final AtomicInteger successfulReads = new AtomicInteger(0);

		for (int i = 0; i < numReaders; i++) {
			executor.submit(() -> {
				try {
					readersReady.countDown();
					startLatch.await();

					try (final LockedFileChannel lock = FILE_LOCK_MANAGER.lockForReading(testFile)) {
						final int concurrent = concurrentReaders.incrementAndGet();
						maxConcurrentReaders.updateAndGet(max -> Math.max(max, concurrent));

						// actually read from the channel
						final byte[] buf = new byte[testContent.length];
						final int bytesRead = lock.read(ByteBuffer.wrap(buf), 0);
						if (bytesRead > 0 && Arrays.equals(buf, testContent)) {
							successfulReads.incrementAndGet();
						}
						Thread.sleep(100);
						concurrentReaders.decrementAndGet();
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					readersFinished.countDown();
				}
			});
		}

		assertTrue("All readers should be ready", readersReady.await(5, TimeUnit.SECONDS));

		final long startTime = System.currentTimeMillis();
		startLatch.countDown();

		assertTrue("All readers should finish", readersFinished.await(10, TimeUnit.SECONDS));
		final long duration = System.currentTimeMillis() - startTime;

		executor.shutdown();
		assertTrue("Executor should terminate", executor.awaitTermination(5, TimeUnit.SECONDS));

		System.out.println("Test completed in " + duration + "ms");
		System.out.println("Maximum concurrent readers: " + maxConcurrentReaders.get());
		System.out.println("Successful reads: " + successfulReads.get());

		assertEquals("All readers should have read successfully", numReaders, successfulReads.get());
		assertTrue("Multiple readers should have been reading concurrently", maxConcurrentReaders.get() > 1);
		assertTrue("Concurrent execution should be faster than sequential", duration < numReaders * 100);
	}

	@Test
	public void testReadWriteExclusion() throws Exception {

		// create a test file
		final Path testFile = tempDir.resolve("test2.txt");
		Files.write(testFile, "initial content".getBytes());

		final String writtenContent = "written by writer";

		// acquire a write lock and write to the file
		final OutputStream writeLock = FILE_LOCK_MANAGER.lockForWriting(testFile).asOutputStream();
		writeLock.write(writtenContent.getBytes());

		// try to acquire a read lock from another thread - should block
		final CountDownLatch readAttempted = new CountDownLatch(1);
		final CountDownLatch readAcquired = new CountDownLatch(1);
		final AtomicReference<String> readContent = new AtomicReference<>();

		new Thread(() -> {
			readAttempted.countDown();
			try (final LockedFileChannel readLock = FILE_LOCK_MANAGER.lockForReading(testFile)) {
				// actually read from the channel
				final byte[] buf = new byte[writtenContent.getBytes().length];
				final int charsRead = readLock.read(ByteBuffer.wrap(buf), 0);
				if (charsRead > 0) {
					readContent.set(new String(buf, 0, charsRead));
				}
				readAcquired.countDown();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}).start();

		assertTrue(readAttempted.await(1, TimeUnit.SECONDS));

		assertFalse("Read lock should not be acquired while write lock is held",
				readAcquired.await(200, TimeUnit.MILLISECONDS));

		writeLock.close();

		assertTrue("Read lock should be acquired after write lock is released",
				readAcquired.await(2, TimeUnit.SECONDS));

		assertEquals("Reader should see written content", writtenContent, readContent.get());
	}

	private class CleanUpHelper implements Closeable {

		private final Path path;
		private final LockedFileChannel channel;

		CleanUpHelper() throws IOException, InterruptedException {
			path = tempDir.resolve("trigger.txt");
			Files.write(path, "trigger".getBytes());
			channel = FILE_LOCK_MANAGER.lockForReading(path);
			tryWaitForSize(-1, 10);
		}

		private void tryWaitForSize(final int expectedSize) throws IOException, InterruptedException {
			tryWaitForSize(expectedSize, 100);
		}

		private void tryWaitForSize(final int expectedSize, final int numIterations) throws IOException, InterruptedException {

			// Wait a bit, trigger GC, loop a few times to try to trigger stale WeakReferences to be processed
			for (int i = 0; i < numIterations; ++i) {
				Thread.sleep(10);
				System.gc();

				// FileKeyLockManager.cleanUp() is called on the side during
				// normal usage. We keep locking and unlocking "trigger.txt" for
				// which we already hold a read lock. This will keep the
				// FileKeyLockManager working, but will not lead to
				// removal/insertion of KeyLockState for "trigger.txt".
				try (LockedFileChannel temp = FILE_LOCK_MANAGER.lockForReading(path)) {
				}

				if (FILE_LOCK_MANAGER.size() == expectedSize) {
					break;
				}
			}
		}

		@Override
		public void close() throws IOException {
			channel.close();
		}
	}

	@Test
	// TODO: Remove? This test relies on garbage collection behaviour and is inherently fragile.
	public void testLockCleanup() throws Exception {

		// create test files
		final Path testFile1 = tempDir.resolve("key1.txt");
		final Path testFile2 = tempDir.resolve("key2.txt");
		final Path testFile3 = tempDir.resolve("key3.txt");
		final byte[] content = "content".getBytes();
		Files.write(testFile1, content);
		Files.write(testFile2, content);
		Files.write(testFile3, content);

		final CleanUpHelper cleanup = new CleanUpHelper();
		final int initialSize = FILE_LOCK_MANAGER.size();

		final LockedFileChannel lock1 = FILE_LOCK_MANAGER.lockForReading(testFile1);
		final OutputStream lock2 = FILE_LOCK_MANAGER.lockForWriting(testFile2).asOutputStream();
		final LockedFileChannel lock3 = FILE_LOCK_MANAGER.lockForReading(testFile3);

		// actually perform I/O on each lock
		final byte[] buf = new byte[content.length];
		lock1.read(ByteBuffer.wrap(buf),0);
		lock2.write(content);
		lock3.read(ByteBuffer.wrap(buf),0);

		assertEquals("Should have 3 new keys", initialSize + 3, FILE_LOCK_MANAGER.size());

		// close lock1 - entry should be auto-removed
		lock1.close();
		cleanup.tryWaitForSize(initialSize + 2);
		assertEquals("key1 should be auto-removed on close", initialSize + 2, FILE_LOCK_MANAGER.size());

		// close remaining locks - entries should be auto-removed
		lock2.close();
		lock3.close();
		cleanup.tryWaitForSize(initialSize);
		assertEquals("All entries should be auto-removed on close", initialSize, FILE_LOCK_MANAGER.size());
	}

	@Test
	public void testWriteLockCreatesFile() throws Exception {

		// file does not exist - write lock creates it via CREATE option
		final Path testFile = tempDir.resolve("newfile.txt");
		final String content = "written to new file";

		assertFalse("File should not exist initially", Files.exists(testFile));

		try (final LockedFileChannel writeLock = FILE_LOCK_MANAGER.lockForWriting(testFile)) {
			assertTrue("File should be created by write lock", Files.exists(testFile));
			// actually write to the file
			writeLock.asOutputStream().write(content.getBytes());
		}

		// verify written content
		assertEquals("Content should be written", content, new String(Files.readAllBytes(testFile)));
	}

	@Test
	public void testWriteLockCreatesParentDirectories() throws Exception {

		// parent directories do not exist - write lock creates them
		final Path testFile = tempDir.resolve("a/b/c/newfile.txt");
		final String content = "written to nested file";

		assertFalse("File should not exist initially", Files.exists(testFile));
		assertFalse("Parent should not exist initially", Files.exists(testFile.getParent()));

		try (final LockedFileChannel writeLock = FILE_LOCK_MANAGER.lockForWriting(testFile)) {
			assertTrue("File should be created by write lock", Files.exists(testFile));
			assertTrue("Parent directories should be created", Files.exists(testFile.getParent()));
			// actually write to the file
			writeLock.asOutputStream().write(content.getBytes());
		}

		// verify written content
		assertEquals("Content should be written", content, new String(Files.readAllBytes(testFile)));
	}

	@Test
	public void testReadLockRequiresExistingFile() throws Exception {
		final Path testFile = tempDir.resolve("nonexistent.txt");
		assertThrows("Should not acquire read lock for non-existent file", NoSuchFileException.class, () -> FILE_LOCK_MANAGER.lockForReading(testFile));
	}

	@Test
	// TODO: Remove? This test relies on garbage collection behaviour and is inherently fragile.
	public void testLocksMapEmptyAfterProperClose() throws Exception {

		final Path testFile = tempDir.resolve("proper-close.txt");
		Files.write(testFile, "content".getBytes());

		final CleanUpHelper cleanup = new CleanUpHelper();
		final int initialSize = FILE_LOCK_MANAGER.size();

		try (final LockedFileChannel lock = FILE_LOCK_MANAGER.lockForWriting(testFile)) {
			lock.asOutputStream().write("new content".getBytes());
		}

		cleanup.tryWaitForSize(initialSize);
		assertEquals("locks map should be back to initial size after proper close",
				initialSize, FILE_LOCK_MANAGER.size());
	}

	@Test
	// TODO: Remove? This test relies on garbage collection behaviour and is inherently fragile.
	public void testLocksMapEmptyAfterLeakedChannelIsGCd() throws Exception {

		final Path testFile = tempDir.resolve("leaked-channel.txt");
		Files.write(testFile, "content".getBytes());

		final CleanUpHelper cleanup = new CleanUpHelper();
		final int initialSize = FILE_LOCK_MANAGER.size();

		// acquire lock in a separate scope so reference can be GC'd
		acquireAndLeakLock(testFile);

		cleanup.tryWaitForSize(initialSize);

		assertEquals("locks map should be back to initial size after leaked channel is GC'd",
				initialSize, FILE_LOCK_MANAGER.size());
	}

	private void acquireAndLeakLock(final Path path) throws IOException {
		// acquire lock but don't close it - just let the reference go out of scope

		@SuppressWarnings("resource")
		LockedFileChannel leaked = FILE_LOCK_MANAGER.lockForWriting(path);
		leaked.asOutputStream().write("leaked content".getBytes());
	}

}
