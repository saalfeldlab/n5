package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
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
		try {
			n5 = new N5FSWriter(relativePath);
			kva = n5.getKeyValueAccess();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testUriParsing() {

		final URI uri = n5.getURI();
		try {
			assertEquals("Container URI must contain scheme", "file", uri.getScheme());

			assertEquals("Container URI must be absolute",
					Paths.get(relativePath).toAbsolutePath().toString(),
					uri.getPath());

			assertEquals("Container URI must be normalized 1", uri, kva.uri(relativeAbnormalPath));
			assertEquals("Container URI must be normalized 2", uri, kva.uri(relativeAbnormalPath2));
			assertEquals("Container URI must be normalized 3", uri, kva.uri("file:" + relativePath));
			assertEquals("Container URI must be normalized 4", uri, kva.uri("file:" + relativeAbnormalPath));
			assertEquals("Container URI must be normalized 5", uri, kva.uri("file:" + relativeAbnormalPath2));

		} catch (URISyntaxException e) {
			fail(e.getMessage());
		}

	}

}
