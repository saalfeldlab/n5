package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import org.janelia.saalfeldlab.n5.DataType;


public class AsTypeCodec implements Codec {

	private static final long serialVersionUID = 1031322606191894484L;

	protected transient final int numBytes;
	protected transient final int numEncodedBytes;

	protected transient final BiConsumer<ByteBuffer, ByteBuffer> encoder;
	protected transient final BiConsumer<ByteBuffer, ByteBuffer> decoder;

	protected final DataType type;
	protected final DataType encodedType;

	public AsTypeCodec( DataType type, DataType encodedType )
	{
		this.type = type;
		this.encodedType = encodedType;

		numBytes = bytes(type);
		numEncodedBytes = bytes(encodedType);

		// TODO fill this out
		if (type == DataType.UINT8 && encodedType == DataType.UINT32) {
			encoder = BYTE_TO_INT;
			decoder = INT_TO_BYTE;
		} else if (type == DataType.UINT32 && encodedType == DataType.UINT8) {
			encoder = INT_TO_BYTE;
			decoder = BYTE_TO_INT;
		} else if (type == DataType.FLOAT64 && encodedType == DataType.INT8) {
			encoder = DOUBLE_TO_BYTE;
			decoder = BYTE_TO_DOUBLE;
		} else if (type == DataType.FLOAT32 && encodedType == DataType.INT8) {
			encoder = FLOAT_TO_BYTE;
			decoder = BYTE_TO_FLOAT;
		} else {
			encoder = IDENTITY;
			decoder = IDENTITY;
		}
	}

	@Override
	public InputStream decode(InputStream in) throws IOException {

		return new FixedLengthConvertedInputStream(numEncodedBytes, numBytes, decoder, in);
	}

	@Override
	public OutputStream encode(OutputStream out) throws IOException {

		return new FixedLengthConvertedOutputStream(numBytes, numEncodedBytes, encoder, out);
	}

	public static int bytes(DataType type) {

		switch (type) {
		case UINT8:
		case INT8:
			return 1;
		case UINT16:
		case INT16:
			return 2;
		case UINT32:
		case INT32:
		case FLOAT32:
			return 4;
		case UINT64:
		case INT64:
		case FLOAT64:
			return 8;
		default:
			return -1;
		}
	}

	public static final BiConsumer<byte[], byte[]> IDENTITY_ARR = (x, y) -> {
		System.arraycopy(x, 0, y, 0, y.length);
	};

	public static final BiConsumer<byte[], byte[]> IDENTITY_ONE_ARR = (x, y) -> {
		y[0] = x[0];
	};

	public static final BiConsumer<byte[], byte[]> BYTE_TO_INT_ARR = (b, i) -> {
		i[0] = 0;
		i[1] = 0;
		i[2] = 0;
		i[3] = b[0];
	};

	public static final BiConsumer<byte[], byte[]> INT_TO_BYTE_ARR = (i, b) -> {
		b[0] = i[3];
	};

	public static final BiConsumer<byte[], byte[]> INT_TO_FLOAT_ARR = (i, f) -> {
		ByteBuffer.wrap(f).putFloat(
				(float)ByteBuffer.wrap(i).getInt());
	};

	public static final BiConsumer<byte[], byte[]> FLOAT_TO_INT_ARR = (f, i) -> {
		ByteBuffer.wrap(i).putInt(
				(int)ByteBuffer.wrap(f).getFloat());
	};

	public static final BiConsumer<byte[], byte[]> INT_TO_DOUBLE_ARR = (i, f) -> {
		ByteBuffer.wrap(f).putDouble(
				(float)ByteBuffer.wrap(i).getInt());
	};

	public static final BiConsumer<byte[], byte[]> DOUBLE_TO_INT_ARR = (f, i) -> {
		ByteBuffer.wrap(i).putInt(
				(int)ByteBuffer.wrap(f).getDouble());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> IDENTITY = (x, y) -> {
		for (int i = 0; i < y.capacity(); i++)
			y.put(x.get());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> IDENTITY_ONE = (x, y) -> {
		y.put(x.get());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> BYTE_TO_INT = (b, i) -> {
		final byte zero = 0;
		i.put(zero);
		i.put(zero);
		i.put(zero);
		i.put(b.get());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> INT_TO_BYTE = (i, b) -> {
		b.put(i.get(3));
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> INT_TO_FLOAT = (i, f) -> {
		f.putFloat((float)i.getInt());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> FLOAT_TO_INT = (f, i) -> {
		i.putInt((int)f.getFloat());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> INT_TO_DOUBLE = (i, f) -> {
		f.putDouble((float)i.getInt());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> DOUBLE_TO_INT = (f, i) -> {
		i.putInt((int)f.getDouble());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> BYTE_TO_FLOAT = (b, f) -> {
		f.putFloat((float)b.get());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> FLOAT_TO_BYTE = (f, b) -> {
		b.put((byte)f.getFloat());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> BYTE_TO_DOUBLE = (b, d) -> {
		d.putDouble((double)b.get());
	};

	public static final BiConsumer<ByteBuffer, ByteBuffer> DOUBLE_TO_BYTE = (d, b) -> {
		b.put((byte)d.getDouble());
	};

}
