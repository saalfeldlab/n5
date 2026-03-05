package org.janelia.saalfeldlab.n5;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

public class FileSystemRootedKeyValueAccess implements RootedKeyValueAccess {


	private final URI root;

	private final String basePath;

	public FileSystemRootedKeyValueAccess(final String basePath) throws N5IOException {
		this.basePath = basePath;
		this.root = new File(basePath).toURI();
	}


	@Override
	public VolatileReadData createReadData(final URI normalPath) throws N5IOException {

		try {
			return _read(root.resolve(normalPath));
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	@Override
	public boolean isDirectory(final URI normalPath) {

		final Path path = Path.of(root.resolve(normalPath));
		return Files.isDirectory(path);
	}

	@Override
	public boolean isFile(final URI normalPath) {

		final Path path = Path.of(root.resolve(normalPath));
		return Files.isRegularFile(path);
	}


	// -- forward to existing IoPolicy interface --
	//    (absolute paths, as strings. maybe revise later...)

	private VolatileReadData _read(final URI uri) throws IOException {

		return ioPolicy.read(new File(uri).getAbsolutePath());
	}

	private final IoPolicy ioPolicy = new FsIoPolicy.Atomic();;

}
