package org.janelia.saalfeldlab.n5.http;

import com.google.gson.GsonBuilder;
import org.janelia.saalfeldlab.n5.AbstractN5Test;
import org.janelia.saalfeldlab.n5.HttpKeyValueAccess;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(RunnerWithHttpServer.class)
public class N5HttpTest extends AbstractN5Test {

	@Parameter
	public static Path httpServerDirectory;

	@Parameter
	public URI httpServerURI;

	@Override
	protected String tempN5Location() {

		try {
			final File tmpFile = Files.createTempFile(httpServerDirectory, "n5-http-test-", ".n5").toFile();
			assertTrue(tmpFile.delete());
			return tmpFile.getName();
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static final ArrayList<N5Writer> tempClassWriters = new ArrayList<>();

	@After
	@Override
	public void removeTempWriters() {

		//For HTTP, don't remove After, remove AfterClass, since we need the server to be shut down first
		// move the writer to a static list
		tempClassWriters.addAll(tempWriters);
		tempWriters.clear();
	}

	@AfterClass
	public static void removeClassTempWriters() {

		for (final N5Writer writer : tempClassWriters) {
			try {
				writer.remove();
			} catch (final Exception e) {
			}
		}
		tempClassWriters.clear();
	}

	private static final boolean cacheMeta = true;

	@Override
	protected N5Writer createN5Writer(
			final String location,
			final GsonBuilder gson) throws IOException {

		final String writerFsPath = httpServerDirectory.resolve(location).toFile().getCanonicalPath();
		final N5FSWriter writer = new N5FSWriter(writerFsPath, gson, cacheMeta);
		final N5KeyValueReader reader = (N5KeyValueReader)createN5Reader(location, gson);
		return new HttpReaderFsWriter(writer, reader);
	}

	@Override
	protected N5Reader createN5Reader(
			final String location,
			final GsonBuilder gson) {

		final String readerHttpPath = httpServerURI.resolve(location).toString();
		return new N5KeyValueReader(new HttpKeyValueAccess(), readerHttpPath, gson, cacheMeta);
	}

	@Test
	@Override
	public void testVersion() throws NumberFormatException {

		try (final N5Writer writer = createTempN5Writer()) {

			final N5Reader.Version n5Version = writer.getVersion();

			assertEquals(n5Version, N5Reader.VERSION);

			final N5Reader.Version incompatibleVersion = new N5Reader.Version(N5Reader.VERSION.getMajor() + 1, N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch());
			writer.setAttribute("/", N5Reader.VERSION_KEY, incompatibleVersion.toString());
			final N5Reader.Version version = writer.getVersion();
			assertFalse(N5Reader.VERSION.isCompatible(version));

			final N5Reader.Version compatibleVersion = new N5Reader.Version(N5Reader.VERSION.getMajor(), N5Reader.VERSION.getMinor(), N5Reader.VERSION.getPatch());
			writer.setAttribute("/", N5Reader.VERSION_KEY, compatibleVersion.toString());
		}
	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testRemoveGroup() {

	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testRemoveAttributes() {

	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testRemoveContainer() {

	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testDelete() {

	}

	@Ignore("N5Writer not supported for HTTP")
	@Override public void testWriterSeparation() {

	}
}
