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
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests multi-process locking behavior of {@link FileKeyLockManager}.
 * <p>
 * These tests fork child processes to verify that file locks work across
 * process boundaries.
 */
public class FileKeyLockManagerMultiProcessTest {

	private FileKeyLockManager lockManager;
	private Path tempDir;
	private Path testFile;

	@Before
	public void setUp() throws IOException {

		lockManager = new FileKeyLockManager();
		tempDir = Files.createTempDirectory("fklm-multiprocess-test");
		testFile = tempDir.resolve("test.txt");
		Files.write(testFile, "test content".getBytes());
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
	public void testWriteLockBlocksOtherProcessWriter() throws Exception {

		try (final LockedChannel writeLock = lockManager.lockForWriting(testFile);
			 final Writer writer = writeLock.newWriter()) {
			writer.write("written by parent");
			writer.flush();

			/* try to acquire write lock from child process - should fail with tryLock */
			final ProcessResult result = runChildProcess("tryWrite", testFile.toString(), "2000");

			assertEquals("Child should fail to acquire write lock", "LOCK_FAILED", result.output.trim());
			assertEquals("Child should exit cleanly", 0, result.exitCode);
		}
	}

	@Test
	public void testWriteLockBlocksOtherProcessReader() throws Exception {

		try (final LockedChannel writeLock = lockManager.lockForWriting(testFile);
			 final Writer writer = writeLock.newWriter()) {
			writer.write("written by parent");
			writer.flush();

			/* try to acquire read lock from child process - should fail with tryLock */
			final ProcessResult result = runChildProcess("tryRead", testFile.toString(), "2000");

			assertEquals("Child should fail to acquire read lock", "LOCK_FAILED", result.output.trim());
			assertEquals("Child should exit cleanly", 0, result.exitCode);
		}
	}

	@Test
	public void testReadLockBlocksOtherProcessWriter() throws Exception {

		try (final LockedChannel readLock = lockManager.lockForReading(testFile);
			 final Reader reader = readLock.newReader()) {
			final char[] buf = new char[100];
			reader.read(buf);

			/* try to acquire write lock from child process - should fail with tryLock */
			final ProcessResult result = runChildProcess("tryWrite", testFile.toString(), "2000");

			assertEquals("Child should fail to acquire write lock", "LOCK_FAILED", result.output.trim());
			assertEquals("Child should exit cleanly", 0, result.exitCode);
		}
	}

	@Test
	public void testReadLockAllowsOtherProcessReader() throws Exception {

		try (final LockedChannel readLock = lockManager.lockForReading(testFile);
			 final Reader reader = readLock.newReader()) {
			final char[] buf = new char[100];
			reader.read(buf);

			/* try to acquire read lock from child process - should succeed */
			final ProcessResult result = runChildProcess("tryRead", testFile.toString(), "2000");

			assertEquals("Child should acquire read lock", "LOCK_ACQUIRED", result.output.trim());
			assertEquals("Child should exit cleanly", 0, result.exitCode);
		}
	}

	@Test
	public void testWriteLockReleasedAllowsOtherProcessWriter() throws Exception {

		try (final LockedChannel writeLock = lockManager.lockForWriting(testFile)) {
			try (final Writer writer = writeLock.newWriter()) {
				writer.write("written by parent");
			}
		}

		/* now child should be able to acquire write lock */
		final ProcessResult result = runChildProcess("tryWrite", testFile.toString(), "2000");

		assertEquals("Child should acquire write lock after release", "LOCK_ACQUIRED", result.output.trim());
		assertEquals("Child should exit cleanly", 0, result.exitCode);
	}

	@Test
	public void testBlockingLockInterruption() throws Exception {

		try (final LockedChannel writeLock = lockManager.lockForWriting(testFile);
			 final Writer writer = writeLock.newWriter()) {
			writer.write("written by parent");
			writer.flush();

			/* child will try blocking lock, then interrupt its own waiting thread */
			final ProcessResult result = runChildProcess("blockingWriteWithInterrupt", testFile.toString(), "500");

			assertTrue("Child should report interruption", result.output.contains("INTERRUPTED"));
			assertTrue("Child should have interrupt flag set", result.output.contains("INTERRUPT_FLAG_SET"));
			assertEquals("Child should exit cleanly", 0, result.exitCode);
		}
	}

	private ProcessResult runChildProcess(final String action, final String filePath, final String timeoutMs) throws Exception {

		final String javaHome = System.getProperty("java.home");
		final String classpath = System.getProperty("java.class.path");
		final String className = LockTestChild.class.getName();

		final ProcessBuilder pb = new ProcessBuilder(
				javaHome + File.separator + "bin" + File.separator + "java",
				"-cp", classpath,
				className,
				action, filePath, timeoutMs
		);

		pb.redirectErrorStream(true);
		final Process process = pb.start();

		final StringBuilder output = new StringBuilder();
		try (final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				output.append(line).append("\n");
			}
		}

		final boolean finished = process.waitFor(10, TimeUnit.SECONDS);
		assertTrue("Child process should finish within timeout", finished);

		return new ProcessResult(process.exitValue(), output.toString());
	}

	private static class ProcessResult {

		final int exitCode;
		final String output;

		ProcessResult(final int exitCode, final String output) {

			this.exitCode = exitCode;
			this.output = output;
		}
	}

	/**
	 * Child process entry point for multi-process lock testing.
	 * <p>
	 * Usage: java LockTestChild &lt;action&gt; &lt;filePath&gt; &lt;timeoutMs&gt;
	 * <p>
	 * Actions:
	 * <ul>
	 *   <li>tryRead - try to acquire a read lock</li>
	 *   <li>tryWrite - try to acquire a write lock</li>
	 *   <li>blockingWriteWithInterrupt - start a thread that blocks on write lock, then interrupt it</li>
	 * </ul>
	 * <p>
	 * Outputs:
	 * <ul>
	 *   <li>LOCK_ACQUIRED - lock was successfully acquired</li>
	 *   <li>LOCK_FAILED - could not acquire lock</li>
	 *   <li>INTERRUPTED - blocking lock was interrupted</li>
	 *   <li>INTERRUPT_FLAG_SET - thread interrupt flag was preserved</li>
	 * </ul>
	 */
	public static class LockTestChild {

		public static void main(final String[] args) {

			if (args.length < 3) {
				System.err.println("Usage: LockTestChild <action> <filePath> <timeoutMs>");
				System.exit(1);
			}

			final String action = args[0];
			final Path filePath = Paths.get(args[1]);
			final long timeoutMs = Long.parseLong(args[2]);

			final FileKeyLockManager lockManager = new FileKeyLockManager();

			try {
				switch (action) {
					case "tryRead":
						handleTryRead(lockManager.tryLockForReading(filePath));
						break;
					case "tryWrite":
						handleTryWrite(lockManager.tryLockForWriting(filePath));
						break;
					case "blockingWriteWithInterrupt":
						handleBlockingWriteWithInterrupt(lockManager, filePath, timeoutMs);
						break;
					default:
						System.err.println("Unknown action: " + action);
						System.exit(1);
						return;
				}

			} catch (final Exception e) {
				System.err.println("Error: " + e.getMessage());
				e.printStackTrace();
				System.exit(2);
			}
		}

		private static void handleTryRead(final LockedChannel lock) throws Exception {

			if (lock != null) {
				try (final Reader reader = lock.newReader()) {
					final char[] buf = new char[100];
					reader.read(buf);
				}
				System.out.println("LOCK_ACQUIRED");
				lock.close();
			} else {
				System.out.println("LOCK_FAILED");
			}
		}

		private static void handleTryWrite(final LockedChannel lock) throws Exception {

			if (lock != null) {
				try (final Writer writer = lock.newWriter()) {
					writer.write("written by child");
				}
				System.out.println("LOCK_ACQUIRED");
				lock.close();
			} else {
				System.out.println("LOCK_FAILED");
			}
		}

		private static void handleBlockingWriteWithInterrupt(
				final FileKeyLockManager lockManager,
				final Path filePath,
				final long timeoutMs) throws Exception {

			final Thread lockThread = new Thread(() -> {
				try {
					lockManager.lockForWriting(filePath);
					System.out.println("LOCK_ACQUIRED");
				} catch (final IOException e) {
					System.out.println("INTERRUPTED");
					if (Thread.currentThread().isInterrupted()) {
						System.out.println("INTERRUPT_FLAG_SET");
					}
				}
			});

			lockThread.start();

			/* wait a bit for the thread to start blocking on the lock */
			Thread.sleep(timeoutMs);

			/* interrupt the waiting thread */
			lockThread.interrupt();

			/* wait for thread to finish */
			lockThread.join(1000);
		}
	}
}
