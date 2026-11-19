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


import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;

import com.google.gson.GsonBuilder;

/**
 * Initiates testing of N5 using an in-memory key value access.
 */
public class N5InMemoryTest extends AbstractN5Test {

	private static final InMemoryKeyValueAccess memAccess = new InMemoryKeyValueAccess();

	private static Random RANDOM = new Random(999);

	private static String tempN5PathName() {
		return "/tmp/n5-test-" + RANDOM.nextLong();
	}

	@Override
	protected String tempN5Location() throws URISyntaxException {

		final String basePath= new File(tempN5PathName()).toURI().normalize().getPath();
		return new URI(null, null, basePath, null).toString();
	}

	@Override protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		return createN5Writer(tempN5Location(), new GsonBuilder());
	}

	@Override
	protected N5Writer createN5Writer(
			final String location,
			final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5KeyValueWriter(memAccess, location, gson, false);
	}

	@Override
	protected N5Reader createN5Reader(
			final String location,
			final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5KeyValueReader(memAccess, location, gson, false);
	}

}
