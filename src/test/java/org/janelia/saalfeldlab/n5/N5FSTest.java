package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.google.gson.GsonBuilder;

/**
 * Initiates testing of the filesystem-based N5 implementation.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 */
public class N5FSTest extends AbstractN5Test {

	private static String tempN5PathName() {

		try {
			final File tmpFile = Files.createTempDirectory("n5-test-").toFile();
			tmpFile.delete();
			tmpFile.mkdir();
			tmpFile.deleteOnExit();
			return tmpFile.getCanonicalPath();
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected String tempN5Location() throws URISyntaxException {

		final String basePath = new File(tempN5PathName()).toURI().normalize().getPath();
		return new URI("file", null, basePath, null).toString();
	}

	@Override
	protected N5Writer createN5Writer() throws IOException, URISyntaxException {

		return createN5Writer(tempN5Location(), new GsonBuilder());
	}

	@Override
	protected N5Writer createN5Writer(
			final String location,
			final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5FSWriter(location, gson);
	}

	@Override
	protected N5Reader createN5Reader(
			final String location,
			final GsonBuilder gson) throws IOException, URISyntaxException {

		return new N5FSReader(location, gson);
	}
}
