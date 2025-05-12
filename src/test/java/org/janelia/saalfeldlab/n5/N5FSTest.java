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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.janelia.saalfeldlab.n5.url.UrlAttributeTest;
import org.junit.Test;

import com.google.gson.GsonBuilder;

/**
 * Initiates testing of the filesystem-based N5 implementation.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 */
public class N5FSTest extends AbstractN5Test {

	private static final FileSystemKeyValueAccess access = new FileSystemKeyValueAccess(FileSystems.getDefault());

	private static String tempN5PathName() {

		try {
			final File tmpFile = Files.createTempDirectory("n5-test-").toFile();
			tmpFile.deleteOnExit();
			return tmpFile.getCanonicalPath();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected String tempN5Location() throws URISyntaxException {

		final String basePath = new File(tempN5PathName()).toURI().normalize().getPath();
		return new URI("file", null, basePath, null).toString();
	}

	@Override protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		return new N5FSWriter(tempN5Location(), new GsonBuilder());
	}

	@Override
	protected N5Writer createN5Writer(
			final String location,
			final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5FSWriter(location, gson);
	}

	@Override
	protected N5Reader createN5Reader(
			final String location,
			final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5FSReader(location, gson);
	}

	@Test
	public void customObjectTest() {

		final String testGroup = "test";
		final ArrayList<TestData<?>> existingTests = new ArrayList<>();

		final UrlAttributeTest.TestDoubles doubles1 = new UrlAttributeTest.TestDoubles(
				"doubles",
				"doubles1",
				new double[]{5.7, 4.5, 3.4});
		final UrlAttributeTest.TestDoubles doubles2 = new UrlAttributeTest.TestDoubles(
				"doubles",
				"doubles2",
				new double[]{5.8, 4.6, 3.5});
		final UrlAttributeTest.TestDoubles doubles3 = new UrlAttributeTest.TestDoubles(
				"doubles",
				"doubles3",
				new double[]{5.9, 4.7, 3.6});
		final UrlAttributeTest.TestDoubles doubles4 = new UrlAttributeTest.TestDoubles(
				"doubles",
				"doubles4",
				new double[]{5.10, 4.8, 3.7});

		try (N5Writer n5 = createTempN5Writer()) {
			n5.createGroup(testGroup);
			addAndTest(n5, existingTests, new TestData<>(testGroup, "/doubles[1]", doubles1));
			addAndTest(n5, existingTests, new TestData<>(testGroup, "/doubles[2]", doubles2));
			addAndTest(n5, existingTests, new TestData<>(testGroup, "/doubles[3]", doubles3));
			addAndTest(n5, existingTests, new TestData<>(testGroup, "/doubles[4]", doubles4));

			/* Test overwrite custom */
			addAndTest(n5, existingTests, new TestData<>(testGroup, "/doubles[1]", doubles4));
		}
	}

//	@Test
	public void testReadLock() throws IOException {

		final Path path = Paths.get(tempN5PathName(), "lock");
		LockedChannel lock = access.lockForWriting(path);
		lock.close();
		lock = access.lockForReading(path);
		System.out.println("locked");

		final ExecutorService exec = Executors.newSingleThreadExecutor();
		final Future<Void> future = exec.submit(() -> {
			access.lockForWriting(path).close();
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

//	@Test
	public void testWriteLock() throws IOException {

		final Path path = Paths.get(tempN5PathName(), "lock");
		final LockedChannel lock = access.lockForWriting(path);
		System.out.println("locked");

		final ExecutorService exec = Executors.newSingleThreadExecutor();
		final Future<Void> future = exec.submit(() -> {
			access.lockForReading(path).close();
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
	public void testLockReleaseByReader() throws IOException, ExecutionException, InterruptedException, TimeoutException {


		final Path path = Paths.get(tempN5PathName(), "lock");
		final LockedChannel lock = access.lockForWriting(path);

		lock.newReader().close();

		final ExecutorService exec = Executors.newSingleThreadExecutor();
		final Future<Void> future = exec.submit(() -> {
			access.lockForWriting(path).close();
			return null;
		});

		future.get(3, TimeUnit.SECONDS);
		future.cancel(true);
		lock.close();
		Files.delete(path);

		exec.shutdownNow();
	}

	@Test
	public void testLockReleaseByInputStream() throws IOException, ExecutionException, InterruptedException, TimeoutException {

		final Path path = Paths.get(tempN5PathName(), "lock");
		final LockedChannel lock = access.lockForWriting(path);

		lock.newInputStream().close();

		final ExecutorService exec = Executors.newSingleThreadExecutor();
		final Future<Void> future = exec.submit(() -> {
			access.lockForWriting(path).close();
			return null;
		});

		future.get(3, TimeUnit.SECONDS);
		future.cancel(true);
		lock.close();
		Files.delete(path);

		exec.shutdownNow();
	}
}
