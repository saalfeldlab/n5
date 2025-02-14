package org.janelia.saalfeldlab.n5.readdata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.commons.io.output.ProxyOutputStream;

class EncodedReadData implements ReadData {

	/**
	 * Like {@code UnaryOperator<OutputStream>}, but {@code apply} throws {@code IOException}.
	 */
	@FunctionalInterface
	public interface OutputStreamOperator {

		OutputStream apply(OutputStream o) throws IOException;

		default OutputStreamOperator andThen(OutputStreamOperator after) {
			Objects.requireNonNull(after);
			return o -> after.apply(apply(o));
		}
	}

	EncodedReadData(final ReadData data, final OutputStreamOperator encoder) {
		this.source = data;
		this.encoder = interceptClose.andThen(encoder)::apply;
	}

	private final ReadData source;

	private final OutputStreamOperator encoder;

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
			try (final OutputStream deflater = encoder.apply(outputStream)) {
				source.writeTo(deflater);
			}
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
