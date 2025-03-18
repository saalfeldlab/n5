package org.janelia.saalfeldlab.n5.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.janelia.saalfeldlab.n5.HttpKeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.junit.Test;

public class HttpKeyValueAccessTest {

	static final String baseUrl = "https://raw.githubusercontent.com/saalfeldlab/n5/afb067678b4827777bb26b6412e7759fb7edee5a/src/test/resources/url/urlAttributes.n5";
	static final String expectedAttributes = "{\"n5\":\"2.6.1\",\"foo\":\"bar\",\"f o o\":\"b a r\",\"list\":[0,1,2,3],\"nestedList\":[[[1,2,3,4]],[[10,20,30,40]],[[100,200,300,400]],[[1000,2000,3000,4000]]],\"object\":{\"a\":\"aa\",\"b\":\"bb\"}}";

	@Test
	public void testExistsRead() {

		final HttpKeyValueAccess kva = new HttpKeyValueAccess();
		final String key = "attributes.json";

		final String absolutePath = kva.compose(baseUrl, key);
		assertTrue(kva.exists(absolutePath));

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
		assertThrows(N5Exception.class, () -> kva.list(""));
		assertThrows(N5Exception.class, () -> kva.listDirectories(""));
		assertThrows(N5Exception.class, () -> kva.delete("foo"));
		assertThrows(N5Exception.class, () -> kva.lockForWriting("bar"));
	}

}