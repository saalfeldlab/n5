package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * This abstract class represents a lazy read operation, and implements the
 * shared logic for lazily reading from a {@link KeyValueAccess}.
 * 
 * @param <K>
 *            the type of {@link KeyValueAccess}.
 */
abstract class KeyValueAccessLazyReadData<K extends KeyValueAccess> implements ReadData {

	protected ReadData materialized;

	protected final K kva;
	protected final String normalKey;
	protected final long offset;
	protected long length;

	KeyValueAccessLazyReadData(K kva, String normalKey, long offset, long length) {

		if (offset < 0)
			throw new IndexOutOfBoundsException("Can not create KeyValueAccesReadData with negative offset: " + offset);

		this.kva = kva;
		this.normalKey = normalKey;
		this.offset = offset;
		this.length = length;
	}

	KeyValueAccessLazyReadData(K kva, String normalKey, long offset) {

		this(kva, normalKey, offset, -1);
	}

	@Override
	public long length() throws N5IOException {

		if (materialized != null)
			return materialized.length();

		if (length < 0) {
			length = kva.size(normalKey);
		}
		return length;
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
	public ReadData materialize() throws N5IOException {

		if (materialized == null)
			read();

		return materialized;
	}

	/**
	 * Read from the backed {@link KeyValueAccess} and set the materialized {@link ReadData} field.
	 *
	 * @throws N5IOException
	 * 		if an I/O error occurs
	 */
	abstract void read() throws N5IOException;

	/**
	 * Return a new instance of a KeyValueAccessLazyReadData for this {@KeyValueAceess} and key,
	 * but that represents a read operation that slices this instance with the given arguments.
	 * <p>
	 * This method should not perform any reads or calls to the backing KeyValueAccess.
	 *
	 * @param offset the offset relative to this
	 * @param length of the returned ReadData
	 * @return 
	 * 		a new KeyValueAccessLazyReadData
	 */
	abstract KeyValueAccessLazyReadData<K> lazySlice(long offset, long length);

	@Override
	public ReadData slice(final long offset, final long length) throws N5IOException {

		if (materialized != null)
			return materialize().slice(offset, length);

		// if a slice of indeterminate length is requested, but the
		// length is already known, use the known length;
		final int lengthArg;
		if (this.length > 0 && length < 0)
			lengthArg = (int)(this.length - offset);
		else
			lengthArg = (int)length;

		return lazySlice(this.offset + offset, lengthArg);
	}

}
