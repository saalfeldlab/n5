package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

abstract class KeyValueAccessReadData<K extends KeyValueAccess> implements ReadData {

	protected ReadData materialized;

	protected final K kva;
	protected final String normalKey;
	protected final long offset;
	protected long length;

	KeyValueAccessReadData(K kva, String normalKey, long offset, long length) {

		this.kva = kva;
		this.normalKey = normalKey;
		this.offset = offset;
		this.length = length;
	}

	KeyValueAccessReadData(K kva, String normalKey, long offset) {

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

		return (ReadData)materialized;
	}

	abstract void read() throws N5IOException;

	abstract KeyValueAccessReadData<K> readOperationSlice(long offset, long length) throws N5IOException;

	@Override
	public ReadData slice(long offset, long length) throws N5IOException {

		if (materialized != null)
			return materialize().slice(offset, length);

		return readOperationSlice(this.offset + offset, length);
	}
}
