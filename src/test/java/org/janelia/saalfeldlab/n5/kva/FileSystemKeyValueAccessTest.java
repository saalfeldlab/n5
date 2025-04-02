/**
 *
 */
package org.janelia.saalfeldlab.n5.kva;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.KeyValueAccess;

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
}
