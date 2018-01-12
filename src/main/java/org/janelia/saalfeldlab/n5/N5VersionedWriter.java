package org.janelia.saalfeldlab.n5;

import java.io.IOException;

public interface N5VersionedWriter extends N5VersionedReader, N5Writer {

	public default void setVersion() throws IOException {

		setAttribute("/", VERSION_KEY, VERSION);
	}
}
