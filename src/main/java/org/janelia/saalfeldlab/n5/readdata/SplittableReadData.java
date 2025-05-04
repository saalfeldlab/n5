package org.janelia.saalfeldlab.n5.readdata;

import java.io.IOException;

public interface SplittableReadData extends ReadData {

	ReadData split(final long offset, final long length) throws IOException;
}
