package org.janelia.saalfeldlab.n5.url;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;

import org.janelia.saalfeldlab.n5.N5URI;
import org.junit.Test;

public class UrlTypeTest {

	@Test
	public void basicUriTypeTests() throws URISyntaxException {

		final String uriFileScheme = "file://a/b/c";
		final String uriS3Scheme = "s3://a/b/c";
		final String uriNoScheme = "/a/b/c";
		final String uriDoubleScheme = "foo:bar:/a/b/c";

		for (String uri : new String[]{uriFileScheme, uriS3Scheme, uriNoScheme, uriDoubleScheme})
			for (String typeScheme : new String[]{ null, N5URI.H5_SCHEME, N5URI.N5_SCHEME, N5URI.ZARR_SCHEME}) {

				final N5URI n5uri = new N5URI((typeScheme == null || typeScheme.isEmpty() ? "" : typeScheme + ":") + uri);

				assertEquals(typeScheme, n5uri.getType());
				assertEquals(uri, n5uri.getContainerPath());
				if( typeScheme != null )
					assertTrue(n5uri.getTypedURI().toString().startsWith( typeScheme));

				assertEquals( 
						typeScheme == null ? uri : typeScheme + ":" + uri, 
						n5uri.getTypedURI().toString());
			}
	}

}
