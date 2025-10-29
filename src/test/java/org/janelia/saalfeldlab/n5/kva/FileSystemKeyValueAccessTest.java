/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.kva;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
public class FileSystemKeyValueAccessTest extends AbstractKeyValueAccessTest {

	/* Weird, but consistent on linux and windows */
	private static URI root = Paths.get(Paths.get("/").toUri()).toUri();

	private static String separator = FileSystems.getDefault().getSeparator();

	private static final FileSystemKeyValueAccess fileSystemKva = new FileSystemKeyValueAccess(FileSystems.getDefault());
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
}
