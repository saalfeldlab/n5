package org.janelia.saalfeldlab.n5.readdata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class EncodedReadData implements ReadData {

	public interface OutputStreamEncoder {

		EncodedOutputStream encode(OutputStream out) throws IOException;
	}

	public interface Finisher {
		void finish() throws IOException;
	}

	public static final class EncodedOutputStream {

		private final OutputStream outputStream;
		private final Finisher finisher;

		public EncodedOutputStream(final OutputStream outputStream, final Finisher finisher) {
			this.outputStream = outputStream;
			this.finisher = finisher;
		}

		OutputStream outputStream() {
			return outputStream;
		}

		void finish() throws IOException {
			finisher.finish();
		}
	}

	private final ReadData source;

	private final OutputStreamEncoder encoder;

	public EncodedReadData(final ReadData data, final OutputStreamEncoder encoder) {
		this.source = data;
		this.encoder = encoder;
	}

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
			final EncodedOutputStream deflater = encoder.encode(outputStream);
			source.writeTo(deflater.outputStream());
			deflater.finish();
		}
	}
}
