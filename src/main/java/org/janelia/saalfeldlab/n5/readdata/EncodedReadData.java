package org.janelia.saalfeldlab.n5.readdata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class EncodedReadData extends AbstractReadData {

	private final ReadData source;

	private final OutputStreamEncoder encoder;

	EncodedReadData(final ReadData data, final OutputStreamEncoder encoder) {
		super(data.order());
		this.source = data;
		this.encoder = encoder;
	}

	private SplittableReadData bytes;

	@Override
	public SplittableReadData splittable() throws IOException {
		if (bytes == null) {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
			writeTo(baos);
			bytes = new ByteArraySplittableReadData(baos.toByteArray()).order(this.order());
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
			final OutputStreamEncoder.EncodedOutputStream deflater = encoder.encode(outputStream);
			source.writeTo(deflater.outputStream());
			deflater.finish();
		}
	}
}
