package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * This abstract class
 * 
 * @param <K> the type of {@link KeyValueAccess}.
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

		// when created with a specified length,
		// need to make sure it is consistent with the actual length
//		final long objLength = kva.size(normalKey);
//		if (length > 0 && offset + length - 1 > objLength)
//			throw new IndexOutOfBoundsException("Object at key: " + normalKey + " has size " + objLength + 
//					". Is too small for  requested offset (" + 
//					offset +") and length (" + length + "). ");

	}

	KeyValueAccessLazyReadData(K kva, String normalKey, long offset) {

		this(kva, normalKey, offset, -1);
	}

	@Override
	public long length() throws IOException {

		if (materialized != null)
			return materialized.length();

		if (length < 0) {
			length = kva.size(normalKey);
		}
		return length;
	}

	@Override
	public InputStream inputStream() throws IOException, IllegalStateException {

		return materialize().inputStream();
	}

	@Override
	public byte[] allBytes() throws IOException, IllegalStateException {

		return materialize().allBytes();
	}

	@Override
	public ReadData materialize() throws IOException {

		if (materialized == null)
			read();

		return materialized;
	}

	abstract void read() throws IOException;

	abstract KeyValueAccessLazyReadData<K> readOperationSlice(long offset, long length) throws IOException;

	@Override
	public ReadData slice(final long offset, final long length) throws IOException {

		if (materialized != null)
			return materialize().slice(offset, length);

		// if a slice of indeterminate length is requested, but the
		// length is already known, use the known length;
		final int lengthArg;
		if (this.length > 0 && length < 0)
			lengthArg = (int)(this.length - offset);
		else
			lengthArg = (int)length;

		return readOperationSlice(this.offset + offset, lengthArg);
	}

	@Override
	public Pair<ReadData, ReadData> split(final long pivot) throws IOException {

		if (materialized != null)
			return materialize().split(pivot);

		final long offsetL = 0;
		final long lenL = pivot;

		final long offsetR = offset + pivot;
		final long lenR = this.length - pivot;

		return new ImmutablePair<ReadData, ReadData>(
				readOperationSlice(offsetL, lenL),
				readOperationSlice(offsetR, lenR));
	}
}
