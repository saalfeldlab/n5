/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
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
package org.janelia.saalfeldlab.n5.http;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.HttpKeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.url.UrlAttributeTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(RunnerWithHttpServer.class)
public class N5HttpTest extends AbstractN5Test {

	@Parameter
	public Path httpServerDirectory;

	@Parameter
	public URI httpServerURI;

	@Override
	protected String tempN5Location() {

		try {
			final File tmpFile = Files.createTempFile(httpServerDirectory, "n5-http-test-", ".n5").toFile();
			assertTrue(tmpFile.delete());
			return tmpFile.getName();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static final ArrayList<N5Writer> tempClassWriters = new ArrayList<>();

	@Override
	@After
	public void removeTempWriters() {

		//For HTTP, don't remove After, remove AfterClass, since we need the server to be shut down first
		// move the writer to a static list
		tempClassWriters.addAll(tempWriters);
		tempWriters.clear();
	}

	@AfterClass
	public static void removeClassTempWriters() {
		for ( final N5Writer writer : tempClassWriters ) {
			try {
				writer.close();
			} catch (final Exception e) {
				fail("Could not close temp writer: " + e.getMessage());
			}
		}
	}



	@Override
	protected N5Writer createN5Writer(
			final String location,
			final GsonBuilder gson) throws IOException {

		final String writerFsPath = httpServerDirectory.resolve(location).toFile().getCanonicalPath();
		final N5FSWriter writer = new N5FSWriter(writerFsPath, gson, false);
		final N5Reader reader = createN5Reader(location, gson);
		return new HttpReaderFsWriter(writer, reader);
	}

	@Override
	protected N5Reader createN5Reader(
			final String location,
			final GsonBuilder gson) {

		final String readerHttpPath = httpServerURI.resolve(location).toString();
		return new N5KeyValueReader(new HttpKeyValueAccess(), readerHttpPath, gson, false);
	}

	@Test
	@Override
	public void testVersion() throws NumberFormatException {

		try (final N5Writer writer = createTempN5Writer()) {

			final N5Reader.Version n5Version = writer.getVersion();

			assertEquals(n5Version, N5Reader.VERSION);

			final N5Reader.Version incompatibleVersion = new N5Reader.Version(N5Reader.VERSION.getMajor() + 1, N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch());
			writer.setAttribute("/", N5Reader.VERSION_KEY, incompatibleVersion.toString());
			final N5Reader.Version version = writer.getVersion();
			assertFalse(N5Reader.VERSION.isCompatible(version));

			assertThrows(N5Exception.N5IOException.class, () -> createTempN5Writer(writer.getURI().getPath().substring(1)));

			final N5Reader.Version compatibleVersion = new N5Reader.Version(N5Reader.VERSION.getMajor(), N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch());
			writer.setAttribute("/", N5Reader.VERSION_KEY, compatibleVersion.toString());
		}
	}

	@Test
	public void customObjectTest() {
		//TODO Caleb: Any reason not to put this in AbstractN5Test?

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
}
