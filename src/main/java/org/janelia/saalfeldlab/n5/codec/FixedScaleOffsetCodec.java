package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

@NameConfig.Name(FixedScaleOffsetCodec.TYPE)
public class FixedScaleOffsetCodec extends AsTypeCodec {

	private static final long serialVersionUID = 8024945290803548528L;

	public static transient final String TYPE = "fixedscaleoffset";

	@NameConfig.Parameter
	protected final double scale;

	@NameConfig.Parameter
	protected final double offset;

	private transient ByteBuffer tmpEncoder;
	private transient ByteBuffer tmpDecoder;

	public transient BiConsumer<ByteBuffer, ByteBuffer> encoder;
	public transient BiConsumer<ByteBuffer, ByteBuffer> encoderPre;
	public transient BiConsumer<ByteBuffer, ByteBuffer> encoderPost;
	public transient BiConsumer<ByteBuffer, ByteBuffer> decoder;
	public transient BiConsumer<ByteBuffer, ByteBuffer> decoderPre;
	public transient BiConsumer<ByteBuffer, ByteBuffer> decoderPost;

	private FixedScaleOffsetCodec() {

		this(1, 0, null, null);
	}

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
			tmpEncoder.rewind();
			tmpEncoder.putDouble(scale * x + offset);
			tmpEncoder.rewind();
			encoderPost.accept(tmpEncoder, o);
		};

		// convert from i type to double, apply scale and offset, then convert to type o
		decoder = (i, o) -> {
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

	@Override
	public String getType() {

		return TYPE;
	}

	@Override public ReadData decode(ReadData readData) throws IOException {

		numBytes = bytes(dataType);
		numEncodedBytes = bytes(encodedType);
		return ReadData.from(new FixedLengthConvertedInputStream(numEncodedBytes, numBytes, this.decoder, readData.inputStream()));
	}

	@Override public ReadData encode(ReadData readData) throws IOException {

		return readData.encode(out -> {

			numBytes = bytes(dataType);
			numEncodedBytes = bytes(encodedType);
			return new FixedLengthConvertedOutputStream(numBytes, numEncodedBytes, this.encoder, out);
		});
	}

}
