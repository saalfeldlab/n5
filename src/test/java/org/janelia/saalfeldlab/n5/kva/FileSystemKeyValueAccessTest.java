package org.janelia.saalfeldlab.n5.kva;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.TestUtils;
import org.janelia.saalfeldlab.n5.TestUtils.WindowsOnly;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class FileSystemKeyValueAccessTest extends AbstractKeyValueAccessTest {

	/* Weird, but consistent on linux and windows */
	private static URI root = Paths.get(Paths.get("/").toUri()).toUri();

	private static String separator = FileSystems.getDefault().getSeparator();

	private static final FileSystemKeyValueAccess fileSystemKva = new FileSystemKeyValueAccess();
	@Override
	protected KeyValueAccess newKeyValueAccess(URI root) {

		return fileSystemKva;
	}

	@Override
	protected KeyValueAccess newKeyValueAccess() {

		return fileSystemKva;
	}

	@Override
	protected URI[] testURIs(URI base) {

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

	@Override
	protected String[][] testPathComponents(URI base) {

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

	@Override
	protected URI tempUri() {

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

	@Override
	@Test
	@Ignore("Empty path is invalid for file URIs.")
	public void testComponentsWithPathEmpty() {
		/* file URIs are purely paths (optional file: scheme) so empty path resolves to a relative path (not the root of the container).
		 * Because of that, there is no valid file URI with an empty path (it's just an empty string, which is invalid, or `file://` which is invalid. */
		super.testComponentsWithPathEmpty();
	}

	@Override
	@Test
	@Ignore("Empty path is invalid for file URIs.")
	public void testComposeWithPathEmpty() {
		/* file URIs are purely paths (optional file: scheme) so empty path resolves to a relative path (not the root of the container).
		 * Because of that, there is no valid file URI with an empty path (it's just an empty string, which is invalid, or `file://` which is invalid. */
		super.testComposeWithPathEmpty();
	}

	@Rule
	public final TestRule windowsOnlyRule = new TestUtils.WindowsOnly.Rule();

	@Test
	public void testUriMatchesPathToUri() throws URISyntaxException {

		final String dir = Paths.get(System.getProperty("java.io.tmpdir"), "a b", "c#d").toString();
		assertEquals(Paths.get(dir).toUri().normalize(), fileSystemKva.uri(dir));
		assertEquals(new File(dir).toPath().toUri(), Paths.get(dir).toUri());
	}

	@Test
	@WindowsOnly
	public void testUNCUriHasAuthority() throws URISyntaxException {

		/* Depending on how you get he UNC path as a string, it could have:
		 * - a legacy version with 4 slashes (no authority, UNC is the path only)
		 * - 2 slashes (network share is the authority, file is the path)
		 *
		 * The FSKVA uri method get the URI from a string, and calls `.normalize()`.
		 * For the 4 slash, no authority version this normalizes to a local root path,
		 * instead of a network mount/share.
		 *
		 * We need to ensure if given a UNC path as string, we should normalize to the
		 * 2 slash authority version, not the 4 slash path-only version */
		final String[] uncPaths = {
				"\\\\server\\share\\dir",
				"//server/share/dir",
				"////server/share/dir",
				"file://server/share/dir"
		};
		for (final String uncPath : uncPaths) {
			final URI uri = fileSystemKva.uri(uncPath);
			final Path expectedPath = uncPath.startsWith("file:") ? Paths.get(URI.create(uncPath)) : Paths.get(uncPath);
			assertEquals(uncPath, "server", uri.getAuthority());
			assertEquals(uncPath, "/share/dir", uri.getPath());
			assertEquals(uncPath, "file://server/share/dir", uri.toString());
			assertEquals(uncPath, expectedPath.toUri().normalize(), uri);
		}
	}

	@Test
	@WindowsOnly
	public void testDifferenceBetweenFileAndPathsURIsWithDriveLetter() throws URISyntaxException {

		/* File(path).toPath().toUri() can differ from Paths.get(path).toUri()
		* We expect the behavior that results:
		* - for UNC paths -> File(path).toPath().toUri()
		* - for all else -> File(path).toURI() */
		for (final String drivePath : new String[]{"/C:/x", "//C:/x", "///C:/x"}) {
			final URI uri = fileSystemKva.uri(drivePath);
			assertNull(drivePath, uri.getAuthority());
			assertEquals(drivePath, "file:/C:/x", uri.toString());
			assertEquals(drivePath, new File(drivePath).toPath().toUri().normalize(), uri);
		}
	}
}
