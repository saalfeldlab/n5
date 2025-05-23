package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.SplittableReadData;

abstract class KeyValueAccessSplittableReadData<K extends KeyValueAccess> implements SplittableReadData {

	protected SplittableReadData materialized;

	protected final K kva;
	protected final String normalKey;
	protected final long offset;
	protected long length;

	public KeyValueAccessSplittableReadData(K kva, String normalKey, long offset, long length) {

		this.kva = kva;
		this.normalKey = normalKey;
		this.offset = offset;
		this.length = length;
	}

	public KeyValueAccessSplittableReadData(K kva, String normalKey, long offset) {

		this(kva, normalKey, offset, -1);
	}

	@Override
	public long length() {

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
	public SplittableReadData materialize() throws IOException {

		if (materialized == null)
			read();

		return (SplittableReadData)materialized;
	}

	abstract void read() throws IOException;

	abstract KeyValueAccessSplittableReadData<K> readOperationSlice(long offset, long length) throws IOException;

	@Override
	public ReadData slice(long offset, long length) throws IOException {

		if (materialized != null)
			return materialize().slice(offset, length);

		return readOperationSlice(this.offset + offset, length);
	}

	@Override
	public Pair<ReadData, ReadData> split(long pivot) throws IOException {

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
