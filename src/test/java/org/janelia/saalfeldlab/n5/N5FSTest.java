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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
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
			tmpFile.delete();
			tmpFile.mkdir();
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

		return createN5Writer(tempN5Location(), new GsonBuilder());
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
	@Ignore("currently working on this")
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

	@Test
	@Ignore("currently working on this")
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
	public void testFSLockRelease() throws IOException, ExecutionException, InterruptedException, TimeoutException {


		final Path path = Paths.get(tempN5PathName(), "lock");
		final ExecutorService exec = Executors.newFixedThreadPool(2);

		// first thread acquires the lock, waits for 200ms then should release it
		exec.submit(() -> {
			try( final LockedChannel lock = access.lockForWriting(path)) {
				lock.newReader();
				Thread.sleep(200);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});

		// second thread waits for the lock.
		// it should get it within a few seconds.
		final Future<Void> future = exec.submit(() -> {
			access.lockForWriting(path).close();
			return null;
		});

		future.get(3, TimeUnit.SECONDS);
		Files.delete(path);
		exec.shutdownNow();
	}
	
	@Test
	public void testReadLockBehavior() throws IOException, InterruptedException, ExecutionException, TimeoutException {

		final Path path = Paths.get(tempN5PathName(), "read-lock");
		path.toFile().createNewFile();

		final ExecutorService exec = Executors.newFixedThreadPool(3);

		final AtomicBoolean v = new AtomicBoolean(false);

		// first thread acquires a read lock, waits for 200ms
		Future<?> f = exec.submit(() -> {
			try( final LockedChannel lock = access.lockForReading(path)) {
				lock.newReader();
				Thread.sleep(200);

				// ensure that the other thread updated the value 
				assertTrue(v.get());

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
			try( final LockedChannel lock = access.lockForReading(path)) {
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


		final Path path = Paths.get(tempN5PathName(), "lock");
		final ExecutorService exec = Executors.newFixedThreadPool(2);

		// first thread acquires the lock, waits for 200ms then should release it
		exec.submit(() -> {
			try( final LockedChannel lock = access.lockForWriting(path)) {
				lock.newReader();
				Thread.sleep(200);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});

		// second thread waits for the lock.
		// it should get it within a few seconds.
		final Future<Void> future = exec.submit(() -> {
			access.lockForWriting(path).close();
			return null;
		});

		future.get(3, TimeUnit.SECONDS);
		Files.delete(path);
		exec.shutdownNow();
	}

}
