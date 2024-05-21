package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import org.janelia.saalfeldlab.n5.DataType;

public class FixedScaleOffsetCodec extends AsTypeCodec {

	private static final long serialVersionUID = 8024945290803548528L;

	@SuppressWarnings("unused")
	private final double scale;

	@SuppressWarnings("unused")
	private final double offset;

	public transient final BiConsumer<ByteBuffer, ByteBuffer> encoder;
	public transient final BiConsumer<ByteBuffer, ByteBuffer> decoder;

	public FixedScaleOffsetCodec(final double scale, final double offset, DataType type, DataType encodedType) {

		super(type, encodedType);
		this.scale = scale;
		this.offset = offset;

		encoder = (f, i) -> {
			final double in = f.getDouble();
			final byte res = (byte)(scale * in + offset);
			i.put((byte)(scale * in + offset));
		};

		decoder = (i, f) -> {
			final byte in = i.get();
			final double conv = (((double)in) - offset) / scale;
			f.putDouble(conv);
		};
	}

	@Override
	public InputStream decode(InputStream in) throws IOException {

		return new FixedLengthConvertedInputStream(numEncodedBytes, numBytes, this.decoder, in);
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		return new FixedLengthConvertedOutputStream(numBytes, numEncodedBytes, this.encoder, out);
	}

}
