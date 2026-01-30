package org.janelia.saalfeldlab.n5.readdata.kva;

import java.io.Closeable;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A lazy reading strategy for lazy, partial reading of data from some source.
 * <p>
 * Implementations of this interface handle the specifics of accessing data from
 * their respective sources.
 *
 * @see LazyReadData
 */
public interface LazyRead extends Closeable {

	/**
	 * Materializes a portion of the data into a concrete {@link ReadData}
	 * instance.
	 * <p>
	 * This method performs the actual read operation from the underlying
	 * source, loading only the requested portion of data. The implementation
	 * should handle bounds checking and throw appropriate exceptions for
	 * invalid ranges.
	 *
	 * @param offset
	 * 		the starting position in the data source
	 * @param length
	 * 		the number of bytes to read, or -1 to read from offset to end
	 *
	 * @return a materialized {@link ReadData} instance containing the requested
	 * data
	 *
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	ReadData materialize(long offset, long length) throws N5IOException;

	/**
	 * Returns the total size of the data source in bytes.
	 *
	 * @return the size of the data source in bytes
	 *
	 * @throws N5IOException
	 * 		if an I/O error occurs while trying to get the length
	 */
	long size() throws N5IOException;

}
