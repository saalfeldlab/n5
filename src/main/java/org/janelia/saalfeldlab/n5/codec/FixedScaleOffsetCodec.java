package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import org.janelia.saalfeldlab.n5.DataType;

public class FixedScaleOffsetCodec extends AsTypeCodec {

	private static final long serialVersionUID = 8024945290803548528L;

	public static transient final String FIXED_SCALE_OFFSET_CODEC_ID = "fixedscaleoffset";

	@SuppressWarnings("unused")
	private final double scale;

	@SuppressWarnings("unused")
	private final double offset;

	protected final String id = FIXED_SCALE_OFFSET_CODEC_ID;

	private transient final ByteBuffer tmpEncoder;
	private transient final ByteBuffer tmpDecoder;

	public transient final BiConsumer<ByteBuffer, ByteBuffer> encoder;
	public transient final BiConsumer<ByteBuffer, ByteBuffer> encoderPre;
	public transient final BiConsumer<ByteBuffer, ByteBuffer> encoderPost;
	public transient final BiConsumer<ByteBuffer, ByteBuffer> decoder;
	public transient final BiConsumer<ByteBuffer, ByteBuffer> decoderPre;
	public transient final BiConsumer<ByteBuffer, ByteBuffer> decoderPost;

	public FixedScaleOffsetCodec(final double scale, final double offset, DataType type, DataType encodedType) {

		super(type, encodedType);
		this.scale = scale;
		this.offset = offset;

		tmpEncoder = ByteBuffer.wrap(new byte[Double.BYTES]);
		tmpDecoder = ByteBuffer.wrap(new byte[Double.BYTES]);

		// encoder goes from type to encoded type
		encoderPre = converter(type, DataType.FLOAT64);
		encoderPost = converter(DataType.FLOAT64, encodedType);

		// decoder goes from encoded type to type
		decoderPre = converter(encodedType, DataType.FLOAT64);
		decoderPost = converter(DataType.FLOAT64, type);

		// convert from i type to double, apply scale and offset, then convert to type o
		encoder = (i, o) -> {
			tmpEncoder.rewind();
			encoderPre.accept(i, tmpEncoder);
			tmpEncoder.rewind();
			final double x = tmpEncoder.getDouble();
			final double y = scale * x + offset;
			System.out.println("encode: " + y);
			tmpEncoder.rewind();
			tmpEncoder.putDouble(scale * x + offset);
			tmpEncoder.rewind();
			encoderPost.accept(tmpEncoder, o);
		};

		// convert from i type to double, apply scale and offset, then convert to type o
		decoder = (i, o) -> {
			// System.out.println("decode");
			// System.out.println(i.capacity());
			// System.out.println(tmpDecoder.capacity());
			// System.out.println(o.capacity());
			tmpDecoder.rewind();
			decoderPre.accept(i, tmpDecoder);
			tmpDecoder.rewind();
			final double x = tmpDecoder.getDouble();
			tmpDecoder.rewind();
			tmpDecoder.putDouble((x - offset) / scale);
			tmpDecoder.rewind();
			decoderPost.accept(tmpDecoder, o);
		};
	}

	public double getScale() {

		return scale;
	}

	public double getOffset() {

		return offset;
	}

	public DataType getType() {

		return super.type;
	}

	public DataType getEncodedType() {

		return encodedType;
	}

	@Override
	public String getId() {

		return id;
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
