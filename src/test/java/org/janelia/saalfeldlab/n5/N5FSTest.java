/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.janelia.saalfeldlab.n5.N5KeyValueReader.LockedFileChannel;
import org.junit.Test;

/**
 * Initiates testing of the filesystem-based N5 implementation.
 *
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 * @author Igor Pisarev &lt;pisarevi@janelia.hhmi.org&gt;
 */
public class N5FSTest extends AbstractN5Test {

	static private String testDirPath = System.getProperty("user.home") + "/tmp/n5-test";

	/**
	 * @throws IOException
	 */
	@Override
	protected N5Writer createN5Writer() throws IOException {

		return new N5FSWriter(testDirPath, false);
	}

	@Test
	public void testReadLock() throws IOException, InterruptedException {

		final Path path = Paths.get(testDirPath, "lock");
		try {
			Files.delete(path);
		} catch (final IOException e) {}

		LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path);
		lockedFileChannel.close();
		lockedFileChannel = LockedFileChannel.openForReading(path);
		System.out.println("locked");

		final ExecutorService exec = Executors.newSingleThreadExecutor();
		final Future<Void> future = exec.submit(() -> {
			LockedFileChannel.openForWriting(path).close();
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
			lockedFileChannel.close();
			Files.delete(path);
		}

		exec.shutdownNow();
	}


	@Test
	public void testWriteLock() throws IOException {

		final Path path = Paths.get(testDirPath, "lock");
		try {
			Files.delete(path);
		} catch (final IOException e) {}

		final LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path);
		System.out.println("locked");

		final ExecutorService exec = Executors.newSingleThreadExecutor();
		final Future<Void> future = exec.submit(() -> {
			LockedFileChannel.openForReading(path).close();
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
			lockedFileChannel.close();
			Files.delete(path);
		}

		exec.shutdownNow();
	}

	@Test
	public void testLockReleaseByReader() throws IOException {

		final Path path = Paths.get(testDirPath, "lock");
		try {
			Files.delete(path);
		} catch (final IOException e) {}

		final LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path);
		System.out.println("locked");

		Channels.newReader(lockedFileChannel.getFileChannel(), StandardCharsets.UTF_8.name()).close();
		System.out.println("reader released");


		final ExecutorService exec = Executors.newSingleThreadExecutor();
		final Future<Void> future = exec.submit(() -> {
			LockedFileChannel.openForWriting(path).close();
			return null;
		});

		try {
			System.out.println("Trying to acquire locked readable channel...");
			future.get(3, TimeUnit.SECONDS);
		} catch (final TimeoutException e) {
			fail("Lock not released!");
			future.cancel(true);
		} catch (final InterruptedException | ExecutionException e) {
			future.cancel(true);
			System.out.println("Test was interrupted!");
		} finally {
			lockedFileChannel.close();
			Files.delete(path);
		}

		System.out.println("Lock was successfully released.");

		exec.shutdownNow();
	}

	@Test
	public void testLockReleaseByInputStream() throws IOException {

		final Path path = Paths.get(testDirPath, "lock");
		try {
			Files.delete(path);
		} catch (final IOException e) {}

		final LockedFileChannel lockedFileChannel = LockedFileChannel.openForWriting(path);
		System.out.println("locked");

		Channels.newInputStream(lockedFileChannel.getFileChannel()).close();
		System.out.println("input stream released");

		final ExecutorService exec = Executors.newSingleThreadExecutor();
		final Future<Void> future = exec.submit(() -> {
			LockedFileChannel.openForWriting(path).close();
			return null;
		});

		try {
			System.out.println("Trying to acquire locked readable channel...");
			future.get(3, TimeUnit.SECONDS);
		} catch (final TimeoutException e) {
			fail("Lock not released!");
			future.cancel(true);
		} catch (final InterruptedException | ExecutionException e) {
			future.cancel(true);
			System.out.println("Test was interrupted!");
		} finally {
			lockedFileChannel.close();
			Files.delete(path);
		}

		System.out.println("Lock was successfully released.");

		exec.shutdownNow();
	}

//	@Test
	public void testCache() {

		final N5Writer n5Writer = n5;
		try {
			n5 = new N5FSWriter(testDirPath + "-cache", true);

			testAttributes();
			testAttributes();

			testCreateDataset();
			testCreateDataset();

			testCreateGroup();
			testCreateGroup();

			testDeepList();
			testDeepList();

			testVersion();
			testVersion();

			testExists();
			testExists();

			testList();
			testList();

			testDelete();
			testDelete();

			testListAttributes();
			testListAttributes();

			testRemove();
			testRemove();

			n5.remove();
		} catch (final IOException e) {
			fail(e.getMessage());
		} finally {
			n5.close();
			n5 = n5Writer;
		}
	}
}
