package org.janelia.saalfeldlab.n5.readdata;

import java.io.IOException;
import java.nio.ByteOrder;

public interface SplittableReadData extends ReadData {

	ReadData split(final long offset, final long length) throws IOException;

	@Override
	SplittableReadData order(ByteOrder byteOrder);
}
