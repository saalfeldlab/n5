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

	protected abstract KeyValueAccess newKeyValueAccess(URI root);
	protected KeyValueAccess newKeyValueAccess() {
		return newKeyValueAccess(tempUri());
	}

	protected abstract URI tempUri();

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
		final URI[] testUris = new URI[pathParts.length ];
		for (int i = 0; i < pathParts.length; i ++) {
			final URI pathPart = pathParts[i];
			testUris[i] = base.resolve(pathPart);
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
			if (uriPath.endsWith("/")) {
				final int lastCompIdx = expectedComponents[i].length - 1;
				final String lastComponent = expectedComponents[i][lastCompIdx];
				if (!lastComponent.endsWith("/")) {
					expectedComponents[i][lastCompIdx] = lastComponent + "/";
				}
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

			assertArrayEquals("Failure at Index " + i ,expectedComponents[i], components);
		}
	}

	protected void testComposeAtLocation(URI uri) {

		final KeyValueAccess access = newKeyValueAccess();

		/* remove any path information to get the base URI without path. */

		final URI[] testUris = testURIs(uri);
		final String[][] testPathComponents = testPathComponents(uri);

		for (int i = 0; i < testPathComponents.length; ++i) {
			testPathComponents[i] = testPathComponents[i].clone();
			final URI baseUri = testUris[i].resolve("/");
			final String[] components = testPathComponents[i];
			final String stringUriFromComponents = access.compose(baseUri, components);
			final URI uriFromComponents = N5URI.getAsUri(stringUriFromComponents);
			assertEquals("Failure at Index " + i , testUris[i], uriFromComponents);
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
