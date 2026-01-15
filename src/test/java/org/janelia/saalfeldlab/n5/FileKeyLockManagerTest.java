/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileKeyLockManagerTest {

	private FileKeyLockManager lockManager;
	private Path tempDir;

	@Before
	public void setUp() throws IOException {

		lockManager = new FileKeyLockManager();
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
							/* ignore */
						}
					});
		}
	}

	@Test
	public void testConcurrentReads() throws Exception {

		/* create a test file */
		final Path testFile = tempDir.resolve("test.txt");
		final String testContent = "test content for concurrent reads";
		Files.write(testFile, testContent.getBytes());

		final int numReaders = 5;
		final ExecutorService executor = Executors.newFixedThreadPool(numReaders);

		final CountDownLatch startLatch = new CountDownLatch(1);
		final CountDownLatch readersReady = new CountDownLatch(numReaders);
		final CountDownLatch readersFinished = new CountDownLatch(numReaders);
		final AtomicInteger concurrentReaders = new AtomicInteger(0);
		final AtomicInteger maxConcurrentReaders = new AtomicInteger(0);
		final AtomicInteger successfulReads = new AtomicInteger(0);

		for (int i = 0; i < numReaders; i++) {
			final int readerId = i;
			executor.submit(() -> {
				try {
					readersReady.countDown();
					startLatch.await();

					try (final LockedChannel lock = lockManager.lockForReading(testFile)) {
						final int concurrent = concurrentReaders.incrementAndGet();
						maxConcurrentReaders.updateAndGet(max -> Math.max(max, concurrent));

						/* actually read from the channel */
						try (final Reader reader = lock.newReader()) {
							final char[] buf = new char[testContent.length()];
							final int charsRead = reader.read(buf);
							if (charsRead > 0 && new String(buf, 0, charsRead).equals(testContent)) {
								successfulReads.incrementAndGet();
							}
						}

						if (concurrent > 1) {
							System.out.println("Reader " + readerId + " reading concurrently with " + (concurrent - 1) + " other readers");
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

		/* create a test file */
		final Path testFile = tempDir.resolve("test2.txt");
		Files.write(testFile, "initial content".getBytes());

		final String writtenContent = "written by writer";

		/* acquire a write lock and write to the file */
		final LockedChannel writeLock = lockManager.lockForWriting(testFile);
		try (final Writer writer = writeLock.newWriter()) {
			writer.write(writtenContent);
		}

		/* try to acquire a read lock from another thread - should block */
		final CountDownLatch readAttempted = new CountDownLatch(1);
		final CountDownLatch readAcquired = new CountDownLatch(1);
		final AtomicReference<String> readContent = new AtomicReference<>();

		new Thread(() -> {
			readAttempted.countDown();
			try (final LockedChannel readLock = lockManager.lockForReading(testFile)) {
				/* actually read from the channel */
				try (final Reader reader = readLock.newReader()) {
					final char[] buf = new char[writtenContent.length()];
					final int charsRead = reader.read(buf);
					if (charsRead > 0) {
						readContent.set(new String(buf, 0, charsRead));
					}
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

	@Test
	public void testTryLock() throws Exception {

		/* create a test file */
		final Path testFile = tempDir.resolve("test3.txt");
		final String testContent = "test content";
		Files.write(testFile, testContent.getBytes());

		/* try acquiring read lock and read */
		LockedChannel readLock1 = lockManager.tryLockForReading(testFile);
		assertNotNull("Should acquire read lock", readLock1);
		try (final Reader reader = readLock1.newReader()) {
			final char[] buf = new char[testContent.length()];
			assertEquals("Should read correct number of chars", testContent.length(), reader.read(buf));
		}

		/* try acquiring another read lock and read */
		LockedChannel readLock2 = lockManager.tryLockForReading(testFile);
		assertNotNull("Should acquire second read lock", readLock2);
		try (final Reader reader = readLock2.newReader()) {
			final char[] buf = new char[testContent.length()];
			assertEquals("Should read correct number of chars", testContent.length(), reader.read(buf));
		}

		/* try acquiring write lock while reads are held - should fail */
		LockedChannel writeLock = lockManager.tryLockForWriting(testFile);
		assertNull("Should not acquire write lock while reads are held", writeLock);

		readLock1.close();
		readLock2.close();

		/* now try write lock and write */
		writeLock = lockManager.tryLockForWriting(testFile);
		assertNotNull("Should acquire write lock after reads are released", writeLock);
		final String newContent = "new content";
		try (final Writer writer = writeLock.newWriter()) {
			writer.write(newContent);
		}

		/* try acquiring read lock from another thread while write is held */
		final AtomicReference<LockedChannel> readLock3 = new AtomicReference<>();
		final Thread readerThread = new Thread(() -> {
			readLock3.set(lockManager.tryLockForReading(testFile));
		});
		readerThread.start();
		readerThread.join();

		assertNull("Should not acquire read lock while write is held", readLock3.get());

		writeLock.close();

		/* verify written content */
		try (final LockedChannel verifyLock = lockManager.lockForReading(testFile);
			 final Reader reader = verifyLock.newReader()) {
			final char[] buf = new char[newContent.length()];
			reader.read(buf);
			assertEquals("Content should match what was written", newContent, new String(buf));
		}
	}

	@Test
	public void testLockCleanup() throws Exception {

		/* create test files */
		final Path testFile1 = tempDir.resolve("key1.txt");
		final Path testFile2 = tempDir.resolve("key2.txt");
		final Path testFile3 = tempDir.resolve("key3.txt");
		final String content = "content";
		Files.write(testFile1, content.getBytes());
		Files.write(testFile2, content.getBytes());
		Files.write(testFile3, content.getBytes());


		final LockedChannel lock1 = lockManager.lockForReading(testFile1);
		final LockedChannel lock2 = lockManager.lockForWriting(testFile2);
		final LockedChannel lock3 = lockManager.lockForReading(testFile3);

		/* actually perform I/O on each lock */
		try (final Reader reader = lock1.newReader()) {
			final char[] buf = new char[content.length()];
			reader.read(buf);
		}
		try (final Writer writer = lock2.newWriter()) {
			writer.write("new content");
		}
		try (final Reader reader = lock3.newReader()) {
			final char[] buf = new char[content.length()];
			reader.read(buf);
		}

		assertEquals("Should have 3 keys", 3, lockManager.size());

		lock1.close();

		final String key1 = testFile1.toAbsolutePath().toString();
		final String key2 = testFile2.toAbsolutePath().toString();
		final String key3 = testFile3.toAbsolutePath().toString();
		assertTrue("Should remove key1", lockManager.removeLockIfUnused(key1));
		assertFalse("Should not remove key2 (write locked)", lockManager.removeLockIfUnused(key2));
		assertFalse("Should not remove key3 (read locked)", lockManager.removeLockIfUnused(key3));

		lock2.close();
		lock3.close();

		final int removed = lockManager.clearUnusedLocks();
		assertEquals("Should remove 2 locks", 2, removed);
		assertEquals("Should have no keys left", 0, lockManager.size());
	}

	@Test
	public void testWriteLockCreatesFile() throws Exception {

		/* file does not exist - write lock creates it via CREATE option */
		final Path testFile = tempDir.resolve("newfile.txt");
		final String content = "written to new file";

		assertFalse("File should not exist initially", Files.exists(testFile));

		try (final LockedChannel writeLock = lockManager.lockForWriting(testFile)) {
			assertTrue("File should be created by write lock", Files.exists(testFile));
			/* actually write to the file */
			try (final Writer writer = writeLock.newWriter()) {
				writer.write(content);
			}
		}

		/* verify written content */
		assertEquals("Content should be written", content, new String(Files.readAllBytes(testFile)));
	}

	@Test
	public void testWriteLockCreatesParentDirectories() throws Exception {

		/* parent directories do not exist - write lock creates them */
		final Path testFile = tempDir.resolve("a/b/c/newfile.txt");
		final String content = "written to nested file";

		assertFalse("File should not exist initially", Files.exists(testFile));
		assertFalse("Parent should not exist initially", Files.exists(testFile.getParent()));

		try (final LockedChannel writeLock = lockManager.lockForWriting(testFile)) {
			assertTrue("File should be created by write lock", Files.exists(testFile));
			assertTrue("Parent directories should be created", Files.exists(testFile.getParent()));
			/* actually write to the file */
			try (final Writer writer = writeLock.newWriter()) {
				writer.write(content);
			}
		}

		/* verify written content */
		assertEquals("Content should be written", content, new String(Files.readAllBytes(testFile)));
	}

	@Test
	public void testReadLockRequiresExistingFile() throws Exception {

		/* file does not exist */
		final Path testFile = tempDir.resolve("nonexistent.txt");
		final String key = testFile.toAbsolutePath().toString();

		LockedChannel readLock = lockManager.tryLockForReading(testFile);
		assertNull("Should not acquire read lock for non-existent file", readLock);
	}

	private void assertFalse(final String message, final boolean condition) {

		assertTrue(message, !condition);
	}
}
