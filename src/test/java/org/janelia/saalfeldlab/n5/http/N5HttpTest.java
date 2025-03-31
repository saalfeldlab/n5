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
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(RunnerWithHttpServer.class)
public class N5HttpTest extends AbstractN5Test {

	@Parameter
	public static Path httpServerDirectory;

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

	@After
	@Override
	public void removeTempWriters() {

		//For HTTP, don't remove After, remove AfterClass, since we need the server to be shut down first
		// move the writer to a static list
		tempClassWriters.addAll(tempWriters);
		tempWriters.clear();
	}

	@AfterClass
	public static void removeClassTempWriters() {

		for (final N5Writer writer : tempClassWriters) {
			try {
				writer.remove();
			} catch (final Exception e) {
			}
		}
		tempClassWriters.clear();
	}

	private static final boolean cacheMeta = true;

	@Override
	protected N5Writer createN5Writer(
			final String location,
			final GsonBuilder gson) throws IOException {

		final String writerFsPath = httpServerDirectory.resolve(location).toFile().getCanonicalPath();
		final N5FSWriter writer = new N5FSWriter(writerFsPath, gson, cacheMeta);
		final N5KeyValueReader reader = (N5KeyValueReader)createN5Reader(location, gson);
		return new HttpReaderFsWriter(writer, reader);
	}

	@Override
	protected N5Reader createN5Reader(
			final String location,
			final GsonBuilder gson) {

		final String readerHttpPath = httpServerURI.resolve(location).toString();
		return new N5KeyValueReader(new HttpKeyValueAccess(), readerHttpPath, gson, cacheMeta);
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

			final N5Reader.Version compatibleVersion = new N5Reader.Version(N5Reader.VERSION.getMajor(), N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch());
			writer.setAttribute("/", N5Reader.VERSION_KEY, compatibleVersion.toString());
		}
	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testRemoveGroup() {

	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testRemoveAttributes() {

	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testRemoveContainer() {

	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testDelete() {

	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testWriterSeparation() {

	}
}
