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
			URI testUri = testUris[i];
			addRelativeUris[i * 3] = testUri;
			Path asPath = Paths.get(testUri);
			final URI schemeLess = asPath.toUri();
			addRelativeUris[i * 3 + 1] = schemeLess;
			URI relativeUri = N5URI.encodeAsUriPath(testUri.getPath().replaceAll("^"+base.getPath(), ""));
			addRelativeUris[i * 3 + 2] = relativeUri;
		}
		return addRelativeUris;
	}

	@Override
	protected void testComponentsAtLocation(URI testRoot) {

		final KeyValueAccess access = newKeyValueAccess();

		final URI[] testPaths = testURIs(testRoot);
		final String[][] expectedComponents = testPathComponents(testRoot);

		for (int i = 0; i < testPaths.length; ++i) {

			String pathString;
			if (testPaths[i].isAbsolute())
				pathString = Paths.get(testPaths[i]).toString();
			else
				pathString = Paths.get(testPaths[i].getPath()).toString();

			if (pathString.length() > 1 && testPaths[i].toString().endsWith("/"))
				pathString += "/";
			final String[] components = access.components(pathString);

			assertArrayEquals("Failure at Index " + i ,expectedComponents[i], components);
		}
	}

	private Path getPathFromFileURI(URI fileUri) {

		try {
			return new File(fileUri).toPath();
		} catch (Exception ignore) {

		}
		try {
			return new File(fileUri.getPath()).toPath();
		} catch (Exception ignore) {

		}
		throw new IllegalArgumentException("Unable to get Path for URI: " + fileUri);
	}

	@Override protected String[][] testPathComponents(URI base) {

		final URI[] testPaths = testURIs(base);
		final String[][] expectedComponents = new String[testPaths.length][];
		for (int i = 0; i < testPaths.length; ++i) {
			final URI testUri = testPaths[i];
			final String testPathStr = testUri.getPath();
			final Path testPath = getPathFromFileURI(testUri);
			final int numComponents = (testPath.getRoot() != null ? 1 : 0) + testPath.getNameCount();
			final String[] components = new String[numComponents];
			int cIdx = 0;
			if (testPath.getRoot() != null)
				components[cIdx++] = testPath.getRoot().toString();

			for (int nameIdx = 0; nameIdx < testPath.getNameCount(); nameIdx++) {
				components[cIdx++] = testPath.getName(nameIdx).toString();
			}

			if (components.length > 0 && (testPath.getRoot()==null || !components[components.length - 1].equals(testPath.getRoot().toString())) && testPathStr.endsWith("/")) {
				final int lastCompIdx = components.length - 1;
				final String lastComponent = components[lastCompIdx];
				if (!lastComponent.endsWith("/")) {
					components[lastCompIdx] = lastComponent + "/";
				}
			}
			expectedComponents[i] = components;
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
			final String composedKey = access.compose(baseUri, components);
			final URI absoluteUri = testUris[i].isAbsolute() ? testUris[i] : uri.resolve("/").resolve(testUris[i]);
			final String testPath = FileSystems.getDefault().provider().getPath(absoluteUri).toAbsolutePath().toString();
			assertEquals("Failure at Index " + i , testPath, composedKey);
		}
	}
}
