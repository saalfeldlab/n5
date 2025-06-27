package org.janelia.saalfeldlab.n5;

import java.io.InputStream;

import org.janelia.saalfeldlab.n5.KeyValueAccess.LazyRead;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A {@link ReadData} implementation that reads from a {@link KeyValueAccess}
 * backend through a {@link LazyRead} object.
 */
public class KeyValueAccessReadData implements ReadData {

    private final LazyRead lazyRead;
    private ReadData materialized;
    private final long offset;
    private long length;

    KeyValueAccessReadData(LazyRead lazyRead) {
        this(lazyRead, 0, -1);
    }

    KeyValueAccessReadData(final LazyRead lazyRead, final long offset, final long length) {
        this.lazyRead = lazyRead;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public ReadData materialize() throws N5IOException {
        if (materialized == null)
            materialized = lazyRead.materialize(offset, length);
        return materialized;
    }

	/**
	 * Returns a {@link ReadData} whose length is limited to the given value.
	 * <p>
	 * This implementation defers a material read operation if allowed
	 * by the {@link LazyRead}.
	 *
	 * @param length
	 *            the length of the resulting ReadData
	 * @return a length-limited ReadData
	 * @throws N5IOException
	 *             if an I/O error occurs while trying to get the length
	 */
    @Override
    public ReadData slice(final long offset, final long length) throws N5IOException {
        if (offset < 0)
            throw new IndexOutOfBoundsException("Negative offset: " + offset);
            
        if (materialized != null)
            return materialized.slice(offset, length);

        // if a slice of indeterminate length is requested, but the
        // length is already known, use the known length;
        final int lengthArg;
        if (this.length > 0 && length < 0)
            lengthArg = (int)(this.length - offset);
        else
            lengthArg = (int)length;

        return new KeyValueAccessReadData(lazyRead, this.offset + offset, lengthArg);
    }

    @Override
    public InputStream inputStream() throws N5IOException, IllegalStateException {
        return materialize().inputStream();
    }

    @Override
    public byte[] allBytes() throws N5IOException, IllegalStateException {
        return materialize().allBytes();
    }

    @Override
    public long length() throws N5IOException {
        if (materialized != null)
            return materialized.length();
        if (length < 0) {
            length = lazyRead.size() - offset;
        }
        return length;
    }

}
