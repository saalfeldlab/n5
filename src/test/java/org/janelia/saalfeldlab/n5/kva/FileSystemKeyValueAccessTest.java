/**
 *
 */
package org.janelia.saalfeldlab.n5.kva;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5URI;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class FileSystemKeyValueAccessTest extends AbstractKeyValueAccessTest {

	/* Weird, but consistent on linux and windows */
	private static URI root = Paths.get(Paths.get("/").toUri()).toUri();

	private static String separator = FileSystems.getDefault().getSeparator();

	private static final FileSystemKeyValueAccess fileSystemKva = new FileSystemKeyValueAccess(FileSystems.getDefault());
	@Override KeyValueAccess newKeyValueAccess(URI root) {

		return fileSystemKva;
	}

	@Override protected KeyValueAccess newKeyValueAccess() {

		return fileSystemKva;
	}

	@Override protected URI[] testURIs(URI base) {

		final URI[] testUris = super.testURIs(base);
		final URI[] addRelativeUris = new URI[testUris.length * 3];
		for (int i = 0; i < testUris.length; i++) {
			addRelativeUris[i * 3] = testUris[i];
			addRelativeUris[i * 3 + 1] = N5URI.encodeAsUriPath(testUris[i].getPath());
			addRelativeUris[i * 3 + 2] = N5URI.encodeAsUriPath(addRelativeUris[i * 2 + 1].getPath().substring(1));
		}
		return addRelativeUris;
	}

	@Override protected String[][] testPathComponents(URI base) {

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

	@Override URI tempUri() {

		try {
			final Path tempDirectory = Files.createTempDirectory("n5-filesystem-kva-test-");
			final File tmpDir = tempDirectory.toFile();
			tmpDir.delete();
			tmpDir.mkdir(); //DeleteOnExit doesn't work on temp directory... so we delete and make it explicitly.
			tmpDir.deleteOnExit();
			return tempDirectory.toUri() ;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	protected void testComposeAtLocation(URI uri) {

		final KeyValueAccess access = newKeyValueAccess();

		/* remove any path information to get the base URI without path. */

		final URI[] testUris = testURIs(uri);
		final String[][] testPathComponents = testPathComponents(uri);

		for (int i = 0; i < testPathComponents.length; ++i) {
			final URI baseUri = testUris[i].resolve("/");
			final String[] components = testPathComponents[i];
			final String stringUriFromComponents = access.compose(baseUri, components);
			final URI uriFromComponents = N5URI.getAsUri(stringUriFromComponents);
			final URI absoluteUri = testUris[i].isAbsolute() ? testUris[i] : uri.resolve("/").resolve(testUris[i]);
			final String testPath = FileSystems.getDefault().provider().getPath(absoluteUri).toString();
			final URI testUri = N5URI.getAsUri(testPath);
			assertEquals("Failure at Index " + i , testUri, uriFromComponents);
		}
	}
}
