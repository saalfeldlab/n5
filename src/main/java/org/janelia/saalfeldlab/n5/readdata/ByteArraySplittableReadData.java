package org.janelia.saalfeldlab.n5.readdata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.util.Arrays;

class ByteArraySplittableReadData extends AbstractReadData implements SplittableReadData {

	private final byte[] data;
	private final int offset;
	private final int length;

	ByteArraySplittableReadData(final byte[] data) {
		this(data, 0, data.length);
	}

	ByteArraySplittableReadData(final byte[] data, final int offset, final int length) {
		this.data = data;
		this.offset = offset;
		this.length = length;
	}

	@Override
	public long length() {
		return length;
	}

	@Override
	public InputStream inputStream() throws IOException {
		return new ByteArrayInputStream(data, offset, length);
	}

	@Override
	public byte[] allBytes() {
		if (offset == 0 && data.length == length) {
			return data;
		} else {
			return Arrays.copyOfRange(data, offset, offset + length);
		}
	}

	@Override
	public SplittableReadData splittable() throws IOException {
		return this;
	}

	@Override
	public SplittableReadData split(final long offset, final long length) {
		if (offset < 0 || offset > this.length || length < 0) {
			throw new IndexOutOfBoundsException();
		}
		final int o = this.offset + (int) offset;
		final int l = Math.min((int) length, this.length - o);
		return new ByteArraySplittableReadData(data, o, l).order(this.order());
	}

	@Override
	public SplittableReadData order(final ByteOrder byteOrder) {
		super.order(byteOrder);
		return this;
	}
}
