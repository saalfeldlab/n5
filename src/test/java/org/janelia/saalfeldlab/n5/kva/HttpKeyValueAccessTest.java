/**
 *
 */
package org.janelia.saalfeldlab.n5.kva;

import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.HttpKeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.http.RunnerWithHttpServer;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.URI;
import java.nio.file.Path;

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
	@Override protected KeyValueAccess newKeyValueAccess(URI root) {

		return httpKva;
	}

	@Override protected KeyValueAccess newKeyValueAccess() {

		return httpKva;
	}

	@Override protected URI tempUri() {

		final URI tmpUri = AbstractN5Test.createTempUri("n5-http-kva-test-", null, httpServerURI);
		return tmpUri;
	}
}
