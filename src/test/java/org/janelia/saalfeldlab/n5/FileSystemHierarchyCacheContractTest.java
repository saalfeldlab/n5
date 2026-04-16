package org.janelia.saalfeldlab.n5;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.janelia.saalfeldlab.n5.cache.HierarchyStore;
import org.janelia.saalfeldlab.n5.cache.AbstractHierarchyCacheContractTest;

public class FileSystemHierarchyCacheContractTest extends AbstractHierarchyCacheContractTest {

	@Override
	protected HierarchyStore createStore() {

		try {
			final Path tempDirectory = Files.createTempDirectory("n5-filesystem-store-contract-test-");
			final File tmpDir = tempDirectory.toFile();
			tmpDir.delete();
			tmpDir.mkdir(); //DeleteOnExit doesn't work on temp directory... so we delete and make it explicitly.
			tmpDir.deleteOnExit();
			return new KeyValueAccessMetaStore(new FileSystemKeyValueRoot(tmpDir.getAbsolutePath()));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}


}
