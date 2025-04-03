/**
 *
 */
package org.janelia.saalfeldlab.n5.kva;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.HttpKeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.http.RunnerWithHttpServer;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Stephan Saalfeld &lt;saalfelds@janelia.hhmi.org&gt;
 *
 */
@RunWith(RunnerWithHttpServer.class)
public class HttpKeyValueAccessTest extends AbstractKeyValueAccessTest {

	@Parameterized.Parameter
	public static Path httpServerDirectory;

	@Parameterized.Parameter
	public URI httpServerURI;

	private static final HttpKeyValueAccess httpKva = new HttpKeyValueAccess();
	@Override KeyValueAccess newKeyValueAccess(URI root) {

		return httpKva;
	}

	@Override protected KeyValueAccess newKeyValueAccess() {

		return httpKva;
	}

	@Override URI tempUri() {

		final URI tmpUri = AbstractN5Test.createTempUri("n5-http-kva-test-", null, httpServerURI);
		return tmpUri;
	}
}
