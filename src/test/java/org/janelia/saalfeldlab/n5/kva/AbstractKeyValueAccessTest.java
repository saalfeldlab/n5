/**
 *
 */
package org.janelia.saalfeldlab.n5.kva;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5URI;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 */
public abstract class AbstractKeyValueAccessTest {

	abstract KeyValueAccess newKeyValueAccess(URI root);
	protected KeyValueAccess newKeyValueAccess() {
		return newKeyValueAccess(tempUri());
	}

	abstract URI tempUri();

	protected URI[] testURIs(final URI base) {
		final URI[] pathParts = new URI[]{
				N5URI.getAsUri("test/path/file"),	 // typical path, with leading and trailing slash
				N5URI.getAsUri("test/path/file/"),	 // typical path, with leading and trailing slash
				N5URI.getAsUri("/test/path/file/"),	 // typical path, with leading and trailing slash
				N5URI.getAsUri("file"),  			 // single path
				N5URI.getAsUri("file/"),			 // single path
				N5URI.getAsUri("/file/"),			 // single path
				N5URI.getAsUri("path/w i t h/spaces"),
				N5URI.getAsUri("uri/illegal%character"),
				N5URI.getAsUri("/"),
				N5URI.getAsUri("")
		};
		final URI[] testUris = new URI[pathParts.length * 2];
		for (int i = 0; i < pathParts.length; i ++) {
			final URI pathPart = pathParts[i];
			testUris[i*2] = base.resolve(pathPart);
			testUris[i*2 + 1] = pathPart;
		}
		return testUris;
	}

	protected String[][] testPathComponents(final URI base) {

		final URI[] testPaths = testURIs(base);
		final String[][] expectedComponents = new String[testPaths.length][];
		for (int i = 0; i < testPaths.length; ++i) {
			final URI testUri = testPaths[i];
			final String uriPath = testUri.getPath();
			expectedComponents[i] = uriPath.split("/");
			if (uriPath.startsWith("/")) {
				//We always expect the first path to be forward slash if it's absolute
				if (expectedComponents[i].length == 0)
					expectedComponents[i] = new String[1];
				expectedComponents[i][0] = "/";
			}
		}
		return expectedComponents;
	}

	protected void testComponentsAtLocation(URI testRoot) {

		final KeyValueAccess access = newKeyValueAccess();

		final URI[] testPaths = testURIs(testRoot);
		final String[][] expectedComponents = testPathComponents(testRoot);

		for (int i = 0; i < testPaths.length; ++i) {

			final String[] components = access.components(testPaths[i].getPath());

			assertArrayEquals(expectedComponents[i], components);
		}
	}

	protected void testComposeAtLocation(URI uri) {

		final KeyValueAccess access = newKeyValueAccess();

		/* remove any path information to get the base URI without path. */
		final URI baseUri = uri.resolve("/");

		final URI[] testUris = testURIs(uri);
		final String[][] testPathComponents = testPathComponents(uri);

		for (int i = 0; i < testPathComponents.length; ++i) {
			final String[] components = testPathComponents[i];
			final String stringUriFromComponents = access.compose(baseUri, components);
			/* A little iffy. We use the same method to get the expected output. Not Ideal, but
			* some KVA do some normalization that we can't anticipate here. The justification
			* for this being acceptable is that we are composing a URI already against a URI,
			* which should always return the second URI (which is our test input) since
			* URIs are required to be absolute. This means ideally the only difference should be
			* whatever normalization occurs. */
			final String stringUriFromTestUri = access.compose(baseUri, testUris[i].toString());
			final URI uriFromTestUri = N5URI.getAsUri(stringUriFromTestUri);
			final String pathFromTestUri = uriFromTestUri.getPath();
			final URI uriFromComponents = N5URI.getAsUri(stringUriFromComponents);
			final String pathFromComponents = uriFromComponents.getPath();
			assertEquals(pathFromTestUri, pathFromComponents);
		}


		/* test string-only compose. Deprecated though, so we may remove this */
		for (int i = 0; i < testPathComponents.length; ++i) {
			final String[] components = testPathComponents[i];
			final String stringUriFromComponents = access.compose(components);
			final String stringUriFromTestUri = access.compose(baseUri, testUris[i].toString());
			final URI uriFromTestUri = N5URI.getAsUri(stringUriFromTestUri);
			String pathFromTestUri = uriFromTestUri.getPath();
			final URI uriFromComponents = N5URI.getAsUri(stringUriFromComponents);
			final String pathFromComponents = uriFromComponents.getPath();
			/* We use the access URI compose method for the expected case, but they differ slightly.
			* the URI compose always will return an absolute URI string, since it's resolving against a
			* URI, which is required to be absolute. But for the string compose, we don't required this.
			* So in the case where the URI made it absolute, but it shouldn't have been, we need to correct
			* expected output to compare the actual output against. */
			if (components.length > 0 && !components[0].equals("/"))
				pathFromTestUri = pathFromTestUri.replaceAll("^/", "");
			assertEquals(pathFromTestUri, pathFromComponents);
		}
	}

	@Test
	public void testComponents() {

		testComponentsAtLocation(tempUri());
	}

	@Test
	public void testComponentsAtRoot() {

		URI root = tempUri().resolve("/");
		testComponentsAtLocation(root);
	}

	@Test
	public void testCompose() {
		testComposeAtLocation(tempUri());
	}

	@Test
	public void testComposeAtRoot() {

		final URI root = tempUri().resolve("/");
		testComposeAtLocation(root);
	}
}
