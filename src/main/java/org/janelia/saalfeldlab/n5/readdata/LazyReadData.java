package org.janelia.saalfeldlab.n5.readdata;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

class LazyReadData implements ReadData {

	LazyReadData(final OutputStreamWriter writer) {
		this.writer = writer;
	}

	private final OutputStreamWriter writer;

	private ByteArraySplittableReadData bytes;

	@Override
	public SplittableReadData splittable() throws IOException {
		if (bytes == null) {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
			writeTo(baos);
			bytes = new ByteArraySplittableReadData(baos.toByteArray());
		}
		return bytes;
	}

	@Override
	public long length() throws IOException {
		return splittable().length();
	}

	@Override
	public InputStream inputStream() throws IOException, IllegalStateException {
		return splittable().inputStream();
	}

	@Override
	public byte[] allBytes() throws IOException, IllegalStateException {
		return splittable().allBytes();
	}

	@Override
	public void writeTo(final OutputStream outputStream) throws IOException, IllegalStateException {
		if (bytes != null) {
			outputStream.write(bytes.allBytes());
		} else {
			writer.writeTo(outputStream);
		}
	}
}
