package org.janelia.saalfeldlab.n5.readdata;

/**
 * A lazy reading strategy for lazy, partial reading of data from some source.
 * <p>
 * Implementations of this interface handle the specifics of accessing data from
 * their respective sources.
 * 
 * @see ReadData
 * @see KeyValueAccessReadData
 */
public interface LazyRead {

    /**
     * Materializes a portion of the data into a concrete {@link ReadData} instance.
     * <p>
     * This method performs the actual read operation from the underlying source,
     * loading only the requested portion of data. The implementation should handle
     * bounds checking and throw appropriate exceptions for invalid ranges.
     *
     * @param offset the starting position in the data source
     * @param length the number of bytes to read, or -1 to read from offset to end
     * @return a materialized {@link ReadData} instance containing the requested data
     */
    ReadData materialize(long offset, long length);

    /**
     * Returns the total size of the data source in bytes.
     * 
     * @return the size of the data source in bytes
     */
    long size();

}
