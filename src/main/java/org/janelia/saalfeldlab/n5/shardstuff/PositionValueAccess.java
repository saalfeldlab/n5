package org.janelia.saalfeldlab.n5.shardstuff;

import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * Idea is to wrap a KeyValueAccess and a dataset URI to be able to get/put values (ReadData) by {@code long[]} key
 */
public interface PositionValueAccess {

	/**
	 * @return ReadData for the given key or {@code null} if the key doesn't exist
	 */
	ReadData get(long[] key) throws N5Exception.N5IOException;

	void put(long[] key, ReadData data) throws N5Exception.N5IOException;

	void remove(long[] key) throws N5Exception.N5IOException;
}
