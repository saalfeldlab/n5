/**
 *
 */
package org.janelia.saalfeldlab.n5.kva;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5URI;
import org.junit.Test;

import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Set;

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

		final Set<URI> testUris = new LinkedHashSet<>();
		/*add the base uri as a test case */
		testUris.add(base);

		/* NOTE: Java 8 doesn't behave well with URIs with empty path when resolving against a path.
		*   See KeyValueAccess#compose for more details.
		* 	In tests with that as a base URI, resolve against `/` first.
		* 	Should be unnecessary in Java 21*/
		final URI testUri = base.getPath().isEmpty() ? base.resolve("/") : base;
		final URI[] pathParts = new URI[]{
				N5URI.getAsUri("test/path/file"),     // typical path, with no leading or trailing slashes
				N5URI.getAsUri("test/path/file/"),     // typical path, with trailing slash
				N5URI.getAsUri("/test/path/file"),     // typical path, with leading slash
				N5URI.getAsUri("/test/path/file/"),     // typical path, with leading and trailing slash
				N5URI.getAsUri("file"),             // single path
				N5URI.getAsUri("file/"),             // single path
				N5URI.getAsUri("/file"),             // single path
				N5URI.getAsUri("/file/"),             // single path
				N5URI.getAsUri("path/w i t h/spaces"),
				N5URI.getAsUri("uri/illegal%character"),
				N5URI.getAsUri("/"), 				// root path
				N5URI.getAsUri("")					// empty path
		};
		for (final URI pathPart : pathParts) {
			testUris.add(testUri.resolve(pathPart));
		}
		return testUris.toArray(new URI[0]);
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

			assertArrayEquals("Failure at Index " + i, expectedComponents[i], components);
		}
	}

	protected void testComposeAtLocation(URI uri) {

		final KeyValueAccess access = newKeyValueAccess();

		/* remove any path information to get the base URI without path. */

		final URI[] testUris = testURIs(uri);
		final String[][] testPathComponents = testPathComponents(uri);

		for (int i = 0; i < testPathComponents.length; ++i) {
			testPathComponents[i] = testPathComponents[i].clone();
			/* Don't add the "/" if the input uri path is empty. just use it. Otherwise, remove the parts and start with "/" */
			final URI baseUri = uri.getPath().isEmpty() ? uri : testUris[i].resolve("/");
			final String[] components = testPathComponents[i];
			final String actualCompose = access.compose(baseUri, components);
			final String expectedCompose = access.compose(testUris[i]);
			assertEquals("Failure at Index " + i, expectedCompose, actualCompose);
		}
	}

	@Test
	public void testComponents() {

		testComponentsAtLocation(tempUri());
	}

	@Test
	public void testComponentsWithPathSlash() {

		final URI uriWithPathSlash = setUriPath(tempUri(), "/");
		testComponentsAtLocation(uriWithPathSlash);
	}

	@Test
	public void testComponentsWithPathEmpty() {

		final URI uriWithPathEmpty = setUriPath(tempUri(), "");
		testComponentsAtLocation(uriWithPathEmpty);
	}

	@Test
	public void testCompose() {

		final URI uri = tempUri();
		testComposeAtLocation(uri);

		final KeyValueAccess kva = newKeyValueAccess();
		final URI uriWithPath = setUriPath(uri, "/foo");
		assertEquals("Non-empty Path", "/foo", uriWithPath.getPath());
		assertComposeEquals("Non-empty Path, no empty or slash in components", kva, uriWithPath, "/foo/bar/baz", "bar", "baz");
        assertComposeEquals("Non-empty Path, first components leading slash", kva, uriWithPath, "/bar/baz", "/bar", "baz");
        assertComposeEquals("Non-empty Path, first components slash only", kva, uriWithPath,"/bar/baz", "/", "bar", "baz");
        assertComposeEquals("Non-empty Path, first components slash only", kva, uriWithPath,"/foo/bar/baz", "", "bar", "baz");
        assertComposeEquals("Non-empty Path, first components slash only", kva, uriWithPath,"/bar/baz", "", "/bar", "baz");
        assertComposeEquals("Non-empty Path, null and empty inner components", kva, uriWithPath,"/foo/bar/baz", "bar", null, "", "baz");
        assertComposeEquals("Non-empty Path, null and empty inner components", kva, uriWithPath,"/bar/baz", "/bar", null, "", "baz");
	}

	@Test
	public void testComposeWithPathSlash() {

		final URI uriWithSlashRoot = setUriPath(tempUri(), "/");
		assertEquals("Root (/) Path", "/", uriWithSlashRoot.getPath());
		testComposeAtLocation(uriWithSlashRoot);

		final KeyValueAccess kva = newKeyValueAccess();
		assertComposeEquals("Root (/) Path, no empty or slash in components", kva, uriWithSlashRoot, "/bar/baz", "bar", "baz");
		assertComposeEquals("Root (/) Path, first components leading slash", kva, uriWithSlashRoot, "/bar/baz", "/bar", "baz");
		assertComposeEquals("Root (/) Path, first components slash only", kva, uriWithSlashRoot,"/bar/baz", "/", "bar", "baz");
		assertComposeEquals("Root (/) Path, first components slash only", kva, uriWithSlashRoot,"/bar/baz", "", "bar", "baz");
		assertComposeEquals("Root (/) Path, first components slash only", kva, uriWithSlashRoot,"/bar/baz", "", "/bar", "baz");
		assertComposeEquals("Root (/) Path, null and empty inner components", kva, uriWithSlashRoot,"/bar/baz", "bar", null, "", "baz");
		assertComposeEquals("Root (/) Path, null and empty inner components", kva, uriWithSlashRoot,"/bar/baz", "/bar", null, "", "baz");
	}

	@Test
	public void testComposeWithPathEmpty() {

		final URI uriWithEmptyRoot = setUriPath(tempUri(), "");
		assertEquals("Empty Path", "", uriWithEmptyRoot.getPath());
		testComposeAtLocation(uriWithEmptyRoot);

		final KeyValueAccess kva = newKeyValueAccess();
		assertComposeEquals("Root (/) Path, no empty or slash in components", kva, uriWithEmptyRoot, "/bar/baz", "bar", "baz");
		assertComposeEquals("Root (/) Path, first components leading slash", kva, uriWithEmptyRoot, "/bar/baz", "/bar", "baz");
		assertComposeEquals("Root (/) Path, first components slash only", kva, uriWithEmptyRoot,"/bar/baz", "/", "bar", "baz");
		assertComposeEquals("Root (/) Path, first components slash only", kva, uriWithEmptyRoot,"/bar/baz", "", "bar", "baz");
		assertComposeEquals("Root (/) Path, first components slash only", kva, uriWithEmptyRoot,"/bar/baz", "", "/bar", "baz");
		assertComposeEquals("Root (/) Path, null and empty inner components", kva, uriWithEmptyRoot,"/bar/baz", "bar", null, "", "baz");
		assertComposeEquals("Root (/) Path, null and empty inner components", kva, uriWithEmptyRoot,"/bar/baz", "/bar", null, "", "baz");
	}

	public void assertComposeEquals(String reason, KeyValueAccess kva, URI uri, String absoluteExpectedPath, String... components) {
		String actual = kva.compose(uri, components);
		String expected = kva.compose(uri.resolve(absoluteExpectedPath));
		assertEquals(reason, expected, actual);
	}

	public URI setUriPath(final URI uri, final String path) {

		final URI tempUri = uri.resolve("/");
		final String newUri = tempUri.toString().replaceAll(tempUri.getPath() + "$", path);
		final URI uriWithNewPath = N5URI.getAsUri(newUri);
		assertEquals("setUriPath failed", path, uriWithNewPath.getPath());
		return uriWithNewPath;
	}
}
