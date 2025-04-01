/**
 *
 */
package org.janelia.saalfeldlab.n5.kva;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.junit.Test;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

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
	abstract String[] testPaths(final URI root);

	protected String[][] expectedComponents(final URI root) {

		final String[] testPaths = testPaths(root);
		final String[][] expectedComponents = new String[testPaths.length][];
		for (int i = 0; i < testPaths.length; ++i) {
			final String testPath = testPaths[i];
			expectedComponents[i] = URI.create(testPath).getPath().split("/");
			if (testPath.startsWith("/")) {
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

		final String[] testPaths = testPaths(testRoot);
		final String[][] expectedComponents = expectedComponents(testRoot);

		for (int i = 0; i < testPaths.length; ++i) {

			final String[] components = access.components(testPaths[i]);

			assertArrayEquals(expectedComponents[i], components);
		}
	}

	protected void testComposeAtLocation(URI testRoot) {

		final KeyValueAccess access = newKeyValueAccess();

		final String[] expectedPaths = testPaths(testRoot);
		final String[][] testComponents = expectedComponents(testRoot);

		for (int i = 0; i < testComponents.length; ++i) {
			final String[] components = testComponents[i];
			final String path = access.compose(components);

			assertEquals(expectedPaths[i], path);
		}
	}

	@Test
	public void testComponents() {

		testComponentsAtLocation(tempUri());
	}

	@Test
	public void testComponentsAtRoot() {

		URI rootUri = tempUri().resolve("/");
		testComponentsAtLocation(rootUri);
	}

	@Test
	public void testCompose() {
		testComposeAtLocation(tempUri());
	}

	@Test
	public void testComposeAtRoot() {
		testComposeAtLocation(tempUri().resolve("/"));
	}
}
