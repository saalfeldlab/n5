package org.janelia.saalfeldlab.n5.readdata;

import java.io.IOException;

import org.apache.commons.lang3.tuple.Pair;

public interface SplittableReadData extends ReadData {

	default ReadData limit(final long length) throws IOException {
		return slice(0, length);
	}

	ReadData slice(final long offset, final long length) throws IOException;

	// TODO do we want this? how should it work?
	Pair<ReadData,ReadData> split(final long pivot) throws IOException;
}
