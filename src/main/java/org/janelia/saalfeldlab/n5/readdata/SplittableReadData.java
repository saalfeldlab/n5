package org.janelia.saalfeldlab.n5.readdata;

import java.io.IOException;

import org.apache.commons.lang3.tuple.Pair;

public interface SplittableReadData extends ReadData {

	default ReadData limit(final long length) throws IOException {
		return slice(0, length);
	}

	/**
	 * Returns a new {@link ReadData} representing a slice, or subset
	 * of this ReadData.
	 *
	 * @param offset the offset relative to this
	 * @param length of the returned ReadData
	 * @return a slice
	 * @throws IOException an exception
	 */
	ReadData slice(final long offset, final long length) throws IOException;

	/*
	 * TODO do we want this? how should it work?
	 * this could be useful for infinite data, or data of unknown length?
	 * 
	 * tail below would be equivalent to slice(pivot, -1)
	 */
	Pair<ReadData,ReadData> split(final long pivot) throws IOException;
}
