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

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Test;

public class UriTest {

	private N5FSWriter n5;

	private KeyValueAccess kva;

	private String relativePath = "src/test/resources/url/urlAttributes.n5";

	private String relativeAbnormalPath = "src/test/resources/./url/urlAttributes.n5";

	private String relativeAbnormalPath2 = "src/test/resources/../resources/url/urlAttributes.n5";

	@Before
	public void before() {

		n5 = new N5FSWriter(relativePath);
		kva = n5.getKeyValueAccess();
	}

	@Test
	public void testUriParsing() throws URISyntaxException {

		final URI uri = n5.getURI();
			assertEquals("Container URI must contain scheme", "file", uri.getScheme());

			assertEquals("Container URI must be absolute",
					uri.getPath(),
					Paths.get(relativePath).toAbsolutePath().toUri().normalize().getPath());

		assertEquals("Container URI must be normalized 1", uri, kva.uri(relativePath));
		assertEquals("Container URI must be normalized 2", uri, kva.uri(relativeAbnormalPath));
		assertEquals("Container URI must be normalized 3", uri, kva.uri(relativeAbnormalPath2));
	}

}
