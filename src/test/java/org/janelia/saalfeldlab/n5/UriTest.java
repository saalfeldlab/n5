package org.janelia.saalfeldlab.n5;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UriTest {

	private N5FSWriter n5;

	private final String relativePath = "src/test/resources/url/urlAttributes.n5";

	@Before
	public void before() {

		n5 = new N5FSWriter(relativePath);
	}

	@Test
	public void testUriParsing() throws URISyntaxException {

		final URI uri = n5.getURI();
			assertEquals("Container URI must contain scheme", "file", uri.getScheme());

			assertEquals("Container URI must be absolute",
					uri.getPath(),
					Paths.get(relativePath).toAbsolutePath().toUri().normalize().getPath());
	}

}
