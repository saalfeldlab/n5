package org.janelia.saalfeldlab.n5.readdata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.ProxyOutputStream;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

class LazyGeneratedReadData implements ReadData {

	LazyGeneratedReadData(final ReadData.Generator generator) {
		this.generator = generator;
	}

	/**
	 * Construct a {@code LazyReadData} that uses the given {@code OutputStreamOperator} to
	 * encode the given {@code ReadData}.
	 *
	 * @param data
	 * 		the ReadData to encode
	 * @param encoder
	 * 		OutputStreamOperator to use for encoding
	 */
	LazyGeneratedReadData(final ReadData data, final OutputStreamOperator encoder) {
		this(outputStream -> {
			try (final OutputStream deflater = encoder.apply(interceptClose.apply(outputStream))) {
				data.writeTo(deflater);
			}
		});
	}

	private final ReadData.Generator generator;

	private ByteArrayReadData bytes;

	private long length = -1;

	@Override
	public ReadData materialize() throws N5IOException {
		if (bytes == null) {
			try {
				final ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
				generator.writeTo(baos);
				bytes = new ByteArrayReadData(baos.toByteArray());
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}
		return this;
	}

	@Override
	public long length() {
		return (bytes != null) ? bytes.length() : length;
	}

	@Override
	public long requireLength() throws N5IOException {
		long l = length();
		if ( l >= 0 ) {
			return l;
		} else {
			return materialize().length();
		}
	}

	@Override
	public ReadData slice(final long offset, final long length) throws N5IOException {
		materialize();
		return bytes.slice(offset, length);
	}

	@Override
	public InputStream inputStream() throws N5IOException, IllegalStateException {
		materialize();
		return bytes.inputStream();
	}

	@Override
	public byte[] allBytes() throws N5IOException, IllegalStateException {
		materialize();
		return bytes.allBytes();
	}

	@Override
	public void writeTo(final OutputStream outputStream) throws N5IOException, IllegalStateException {
		try {
			if (bytes != null) {
				outputStream.write(bytes.allBytes());
			} else {
				final CountingOutputStream cos = new CountingOutputStream(outputStream);
				generator.writeTo(cos);
				length = cos.getByteCount();
			}
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	/**
	 * {@code UnaryOperator} that wraps {@code OutputStream} to intercept {@code
	 * close()} and call {@code flush()} instead
	 */
	private static final OutputStreamOperator interceptClose = o -> new ProxyOutputStream(o) {

		@Override
		public void close() throws IOException {
			out.flush();
		}
	};
}
