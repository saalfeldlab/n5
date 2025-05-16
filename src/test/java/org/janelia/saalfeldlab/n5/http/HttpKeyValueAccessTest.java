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
package org.janelia.saalfeldlab.n5.http;

import org.apache.commons.io.IOUtils;
import org.janelia.saalfeldlab.n5.HttpKeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

public class HttpKeyValueAccessTest {

	static final URI baseUrl = URI.create("https://raw.githubusercontent.com/saalfeldlab/n5/afb067678b4827777bb26b6412e7759fb7edee5a/src/test/resources/url/urlAttributes.n5");
	static final String expectedAttributes = "{\"n5\":\"2.6.1\",\"foo\":\"bar\",\"f o o\":\"b a r\",\"list\":[0,1,2,3],\"nestedList\":[[[1,2,3,4]],[[10,20,30,40]],[[100,200,300,400]],[[1000,2000,3000,4000]]],\"object\":{\"a\":\"aa\",\"b\":\"bb\"}}";


	@Test
	public void testExistsRead() {

		final HttpKeyValueAccess kva = new HttpKeyValueAccess();
		final String key = "attributes.json";

		final String absolutePath = kva.compose(baseUrl, key);
		assumeTrue(kva.exists(absolutePath));

		try (LockedChannel ch = kva.lockForReading(absolutePath)) {

			final InputStream is = ch.newInputStream();

			final String attributes = IOUtils.toString(is, Charset.defaultCharset());
			assertEquals(expectedAttributes, attributes);

			is.close();

		} catch (IOException e) {
			// not correct to fail for an IO exception
			e.printStackTrace();
		}
	}

	@Test
	public void testUnsupportedOperations() {

		final HttpKeyValueAccess kva = new HttpKeyValueAccess();
		assertThrows(N5Exception.class, () -> kva.delete("foo"));
		assertThrows(N5Exception.class, () -> kva.lockForWriting("bar"));
	}

}
