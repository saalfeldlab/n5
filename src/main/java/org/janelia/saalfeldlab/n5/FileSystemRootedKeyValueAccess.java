package org.janelia.saalfeldlab.n5;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

class FileSystemRootedKeyValueAccess implements RootedKeyValueAccess {


	private final URI root;

	private final String basePath;

	FileSystemRootedKeyValueAccess(final String basePath) throws N5IOException {
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





	// -- forward to existing IoPolicy interface --
	//    (absolute paths, as strings. maybe revise later...)

	private VolatileReadData _read(final URI uri) throws IOException {

		return ioPolicy.read(new File(uri).getAbsolutePath());
	}

	private final IoPolicy ioPolicy = new FsIoPolicy.Atomic();;

}
