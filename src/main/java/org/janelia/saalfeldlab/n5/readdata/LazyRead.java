package org.janelia.saalfeldlab.n5.readdata;

import java.io.Closeable;
import java.util.Collection;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

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

	/**
	 * Indicates that the given slices will be subsequently read.
	 * {@code LazyRead} implementations (optionally) may take steps to prepare
	 * for these subsequent slices.
	 *
	 * @param ranges
	 * 		slice ranges to prefetch
	 *
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	default void prefetch(final Collection<? extends Range> ranges) throws N5IOException {
	}

	/**
	 * Helper method to perform bounds checking.
	 * Verify that the range {@code [offset, offset+length)} is fully contained in {@code [0, channelSize)}.
	 *
	 * @param channelSize
	 * 		the size of the data source in bytes
	 * @param offset
	 * 		the starting position in the data source
	 * @param length
	 * 		the number of bytes to read, or -1 to read from offset to end
	 *
	 * @throws IndexOutOfBoundsException
	 * 		if range is not fully contained
	 */
	static void validateBounds(final long channelSize, final long offset, final long length) {

		if (offset < 0)
			throw new IndexOutOfBoundsException("offset must be >= 0, but was: " + offset);
		else if (channelSize > 0 && offset >= channelSize) // offset == 0 and dataLength == 0 is okay
			throw new IndexOutOfBoundsException("offset (" + offset + ") must be less than channel size (" + channelSize + ")");
		else if (length >= 0 && offset + length > channelSize)
			throw new IndexOutOfBoundsException("offset + length (" + (offset + length) + ") must be less than channel size (" + channelSize + ")");
	}
}
