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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import org.junit.Test;

public class KeyLockTest {

	@Test
	public void testConcurrentReads() throws InterruptedException {

		KeyLock keyLock = new KeyLock();
		String testKey = "test-key";

		int numReaders = 5;
		ExecutorService executor = Executors.newFixedThreadPool(numReaders);

		// Synchronization primitives
		CountDownLatch startLatch = new CountDownLatch(1);
		CountDownLatch readersReady = new CountDownLatch(numReaders);
		CountDownLatch readersFinished = new CountDownLatch(numReaders);
		AtomicInteger concurrentReaders = new AtomicInteger(0);
		AtomicInteger maxConcurrentReaders = new AtomicInteger(0);

		// Submit reader tasks
		for (int i = 0; i < numReaders; i++) {
			final int readerId = i;
			executor.submit(() -> {
				try {
					// Signal this reader is ready
					readersReady.countDown();

					// Wait for all readers to be ready
					startLatch.await();

					// Acquire read lock
					Lock lock = keyLock.lockForReading(testKey);
					try {
						// Track concurrent readers
						int concurrent = concurrentReaders.incrementAndGet();
						maxConcurrentReaders.updateAndGet(max -> Math.max(max, concurrent));

						if (concurrent > 1) {
							System.out.println("Reader " + readerId + " reading concurrently with " + (concurrent - 1) + " other readers");
						}

						// Simulate some work
						Thread.sleep(100);

						concurrentReaders.decrementAndGet();
					} finally {
						lock.unlock();
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					readersFinished.countDown();
				}
			});
		}

		// Wait for all readers to be ready
		assertTrue("All readers should be ready", readersReady.await(5, TimeUnit.SECONDS));

		// Start all readers at the same time
		long startTime = System.currentTimeMillis();
		startLatch.countDown();

		// Wait for all readers to finish
		assertTrue("All readers should finish", readersFinished.await(5, TimeUnit.SECONDS));
		long duration = System.currentTimeMillis() - startTime;

		executor.shutdown();
		assertTrue("Executor should terminate", executor.awaitTermination(5, TimeUnit.SECONDS));

		// Verify concurrent execution
		System.out.println("Test completed in " + duration + "ms");
		System.out.println("Maximum concurrent readers: " + maxConcurrentReaders.get());

		// With ReentrantReadWriteLock, we should see true concurrent reads
		assertTrue("Multiple readers should have been reading concurrently", maxConcurrentReaders.get() > 1);

		// Time should be much less than sequential execution (numReaders * 100ms)
		assertTrue("Concurrent execution should be faster than sequential", duration < numReaders * 100);
	}

	@Test
	public void testReadWriteExclusion() throws InterruptedException {

		KeyLock keyLock = new KeyLock();
		String testKey = "test-key";

		// First, acquire a write lock
		Lock writeLock = keyLock.lockForWriting(testKey);

		// Try to acquire a read lock from another thread - should block
		CountDownLatch readAttempted = new CountDownLatch(1);
		CountDownLatch readAcquired = new CountDownLatch(1);

		new Thread(() -> {
			readAttempted.countDown();
			Lock readLock = keyLock.lockForReading(testKey);
			try {
				readAcquired.countDown();
			} finally {
				readLock.unlock();
			}
		}).start();

		// Wait for read attempt
		assertTrue(readAttempted.await(1, TimeUnit.SECONDS));

		// Read should not be acquired while write lock is held
		assertFalse("Read lock should not be acquired while write lock is held",
				readAcquired.await(100, TimeUnit.MILLISECONDS));

		// Release write lock
		writeLock.unlock();

		// Now read should be acquired
		assertTrue("Read lock should be acquired after write lock is released",
				readAcquired.await(1, TimeUnit.SECONDS));
	}

	@Test
	public void testTryLock() throws InterruptedException {

		KeyLock keyLock = new KeyLock();
		String testKey = "test-key";

		// Try acquiring read lock - should succeed
		Lock readLock1 = keyLock.tryLockForReading(testKey);
		assertNotNull("Should acquire read lock", readLock1);

		// Try acquiring another read lock - should succeed
		Lock readLock2 = keyLock.tryLockForReading(testKey);
		assertNotNull("Should acquire second read lock", readLock2);

		// Try acquiring write lock while reads are held - should fail
		Lock writeLock = keyLock.tryLockForWriting(testKey);
		assertNull("Should not acquire write lock while reads are held", writeLock);

		// Release read locks
		readLock1.unlock();
		readLock2.unlock();

		// Now try write lock - should succeed
		writeLock = keyLock.tryLockForWriting(testKey);
		assertNotNull("Should acquire write lock after reads are released", writeLock);

		// Try acquiring read lock from another thread while write is held - should fail
		// Because
		AtomicReference<Lock> readLock3 = new AtomicReference<>();
		Thread readerThread = new Thread(() -> {
			readLock3.set(keyLock.tryLockForReading(testKey));
		});
		readerThread.start();
		readerThread.join();

		assertNull("Should not acquire read lock while write is held", readLock3.get());

		writeLock.unlock();
	}

	@Test
	public void testLockCleanup() {

		KeyLock keyLock = new KeyLock();

		// Create some locks
		Lock lock1 = keyLock.lockForReading("key1");
		Lock lock2 = keyLock.lockForWriting("key2");
		Lock lock3 = keyLock.lockForReading("key3");

		assertEquals("Should have 3 keys", 3, keyLock.size());

		// Release lock1
		lock1.unlock();

		// Try to remove unused locks
		assertTrue("Should remove key1", keyLock.removeLockIfUnused("key1"));
		assertFalse("Should not remove key2 (write locked)", keyLock.removeLockIfUnused("key2"));
		assertFalse("Should not remove key3 (read locked)", keyLock.removeLockIfUnused("key3"));

		// Release remaining locks
		lock2.unlock();
		lock3.unlock();

		// Clear all unused locks
		int removed = keyLock.clearUnusedLocks();
		assertEquals("Should remove 2 locks", 2, removed);
		assertEquals("Should have no keys left", 0, keyLock.size());
	}

	private void assertFalse(String message, boolean condition) {

		assertTrue(message, !condition);
	}
}