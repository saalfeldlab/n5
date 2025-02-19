package org.janelia.saalfeldlab.n5.readdata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.output.ProxyOutputStream;

class LazyReadData implements ReadData {

	LazyReadData(final OutputStreamWriter writer) {
		this.writer = writer;
	}

	/**
	 * Construct a {@code LazyReadData} that uses the given {@code OutputStreamEncoder} to
	 * encode the given {@code ReadData}.
	 *
	 * @param data
	 * 		the ReadData to encode
	 * @param encoder
	 * 		OutputStreamEncoder to use for encoding
	 */
	LazyReadData(final ReadData data, final OutputStreamOperator encoder) {
		this(outputStream -> {
			try (final OutputStream deflater = encoder.apply(interceptClose.apply(outputStream))) {
				data.writeTo(deflater);
			}
		});
	}

	private final OutputStreamWriter writer;

	private ByteArraySplittableReadData bytes;

	@Override
	public ReadData materialize() throws IOException {
		if (bytes == null) {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
			writeTo(baos);
			bytes = new ByteArraySplittableReadData(baos.toByteArray());
		}
		return bytes;
	}

	@Override
	public long length() throws IOException {
		return materialize().length();
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
	public void writeTo(final OutputStream outputStream) throws IOException, IllegalStateException {
		if (bytes != null) {
			outputStream.write(bytes.allBytes());
		} else {
			writer.writeTo(outputStream);
		}
	}

	/**
	 * {@code UnaryOperator} that wraps {@code OutputStream} to intercept {@code
	 * close()} and call {@code flush()} instead
	 */
	private static OutputStreamOperator interceptClose = o -> new ProxyOutputStream(o) {

		@Override
		public void close() throws IOException {
			out.flush();
		}
	};
}
