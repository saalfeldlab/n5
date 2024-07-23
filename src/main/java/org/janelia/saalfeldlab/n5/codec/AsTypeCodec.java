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

	protected final String name = "astype";

	public AsTypeCodec( DataType type, DataType encodedType )
	{
		this.type = type;
		this.encodedType = encodedType;

		numBytes = bytes(type);
		numEncodedBytes = bytes(encodedType);

		encoder = converter(type, encodedType);
		decoder = converter(encodedType, type);
	}

	@Override
	public String getName() {

		return name;
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

	public static BiConsumer<ByteBuffer, ByteBuffer> converter(final DataType from, final DataType to) {

		// // TODO fill this out

		if (from == to)
			return AsTypeCodec::IDENTITY;
		else if (from == DataType.INT8) {

			if( to == DataType.INT16 )
				return AsTypeCodec::BYTE_TO_SHORT;
			else if( to == DataType.INT32 )
				return AsTypeCodec::BYTE_TO_INT;
			else if( to == DataType.INT64 )
				return AsTypeCodec::BYTE_TO_LONG;
			else if( to == DataType.FLOAT32 )
				return AsTypeCodec::BYTE_TO_FLOAT;
			else if( to == DataType.FLOAT64 )
				return AsTypeCodec::BYTE_TO_DOUBLE;

		} else if (from == DataType.INT16) {

			if (to == DataType.INT8)
				return AsTypeCodec::SHORT_TO_BYTE;
			else if (to == DataType.INT32)
				return AsTypeCodec::SHORT_TO_INT;
			else if (to == DataType.INT64)
				return AsTypeCodec::SHORT_TO_LONG;
			else if (to == DataType.FLOAT32)
				return AsTypeCodec::SHORT_TO_FLOAT;
			else if (to == DataType.FLOAT64)
				return AsTypeCodec::SHORT_TO_DOUBLE;

		} else if (from == DataType.INT32) {

			if (to == DataType.INT8)
				return AsTypeCodec::INT_TO_BYTE;
			else if (to == DataType.INT16)
				return AsTypeCodec::INT_TO_SHORT;
			if (to == DataType.INT8)
				return AsTypeCodec::DOUBLE_TO_BYTE;
			else if (to == DataType.INT16)
				return AsTypeCodec::DOUBLE_TO_SHORT;
			else if (to == DataType.INT32)
				return AsTypeCodec::DOUBLE_TO_INT;
			else if (to == DataType.INT64)
				return AsTypeCodec::DOUBLE_TO_LONG;
			else if (to == DataType.FLOAT32)
				return AsTypeCodec::DOUBLE_TO_FLOAT;
			else if (to == DataType.INT64)
				return AsTypeCodec::INT_TO_LONG;
			else if (to == DataType.FLOAT32)
				return AsTypeCodec::INT_TO_FLOAT;
			else if (to == DataType.FLOAT64)
				return AsTypeCodec::INT_TO_DOUBLE;

		} else if (from == DataType.INT64) {

			if (to == DataType.INT8)
				return AsTypeCodec::LONG_TO_BYTE;
			else if (to == DataType.INT16)
				return AsTypeCodec::LONG_TO_SHORT;
			else if (to == DataType.INT32)
				return AsTypeCodec::LONG_TO_INT;
			else if (to == DataType.FLOAT32)
				return AsTypeCodec::LONG_TO_FLOAT;
			else if (to == DataType.FLOAT64)
				return AsTypeCodec::LONG_TO_DOUBLE;

		} else if (from == DataType.FLOAT32) {

			if (to == DataType.INT8)
				return AsTypeCodec::FLOAT_TO_BYTE;
			else if (to == DataType.INT16)
				return AsTypeCodec::FLOAT_TO_SHORT;
			else if (to == DataType.INT32)
				return AsTypeCodec::FLOAT_TO_INT;
			else if (to == DataType.INT64)
				return AsTypeCodec::FLOAT_TO_LONG;
			else if (to == DataType.FLOAT64)
				return AsTypeCodec::FLOAT_TO_DOUBLE;

		} else if (from == DataType.FLOAT64) {

			if (to == DataType.INT8)
				return AsTypeCodec::DOUBLE_TO_BYTE;
			else if (to == DataType.INT16)
				return AsTypeCodec::DOUBLE_TO_SHORT;
			else if (to == DataType.INT32)
				return AsTypeCodec::DOUBLE_TO_INT;
			else if (to == DataType.INT64)
				return AsTypeCodec::DOUBLE_TO_LONG;
			else if (to == DataType.FLOAT32)
				return AsTypeCodec::DOUBLE_TO_FLOAT;
		}

		return AsTypeCodec::IDENTITY;
	}

	public static final void IDENTITY(final ByteBuffer x, final ByteBuffer y) {

		for (int i = 0; i < y.capacity(); i++)
			y.put(x.get());
	}

	public static final void IDENTITY_ONE(final ByteBuffer x, final ByteBuffer y) {

		y.put(x.get());
	}

	public static final void BYTE_TO_SHORT(final ByteBuffer b, final ByteBuffer s) {

		final byte zero = 0;
		s.put(zero);
		s.put(b.get());
	}

	public static final void BYTE_TO_INT(final ByteBuffer b, final ByteBuffer i) {

		final byte zero = 0;
		i.put(zero);
		i.put(zero);
		i.put(zero);
		i.put(b.get());
	}

	public static final void BYTE_TO_LONG(final ByteBuffer b, final ByteBuffer l) {

		final byte zero = 0;
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(b.get());
	}

	public static final void BYTE_TO_FLOAT(final ByteBuffer b, final ByteBuffer f) {

		f.putFloat((float)b.get());
	}

	public static final void BYTE_TO_DOUBLE(final ByteBuffer b, final ByteBuffer d) {

		d.putDouble((double)b.get());
	}

	public static final void SHORT_TO_BYTE(final ByteBuffer s, final ByteBuffer b) {

		final byte zero = 0;
		b.put(zero);
		b.put(s.get());
	}

	public static final void SHORT_TO_INT(final ByteBuffer s, final ByteBuffer i) {

		final byte zero = 0;
		i.put(zero);
		i.put(zero);
		i.put(s.get());
		i.put(s.get());
	}

	public static final void SHORT_TO_LONG(final ByteBuffer s, final ByteBuffer l) {

		final byte zero = 0;
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(s.get());
		l.put(s.get());
	}

	public static final void SHORT_TO_FLOAT(final ByteBuffer s, final ByteBuffer f) {

		f.putFloat((float)s.getShort());
	}

	public static final void SHORT_TO_DOUBLE(final ByteBuffer s, final ByteBuffer d) {

		d.putDouble((double)s.getShort());
	}

	public static final void INT_TO_BYTE(final ByteBuffer i, final ByteBuffer b) {

		b.put(i.get(3));
	}

	public static final void INT_TO_SHORT(final ByteBuffer i, final ByteBuffer s) {

		s.put(i.get(2));
		s.put(i.get(3));
	}

	public static final void INT_TO_LONG(final ByteBuffer i, final ByteBuffer l) {

		final byte zero = 0;
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(zero);
		l.put(i.get());
		l.put(i.get());
		l.put(i.get());
		l.put(i.get());
	}

	public static final void INT_TO_FLOAT(final ByteBuffer i, final ByteBuffer f) {

		f.putFloat((float)i.getInt());
	}

	public static final void INT_TO_DOUBLE(final ByteBuffer i, final ByteBuffer f) {

		f.putDouble((float)i.getInt());
	}

	public static final void LONG_TO_BYTE(final ByteBuffer l, final ByteBuffer b) {

		b.put((byte)l.getLong());
	}

	public static final void LONG_TO_SHORT(final ByteBuffer l, final ByteBuffer s) {

		s.putShort((short)l.getLong());
	}

	public static final void LONG_TO_INT(final ByteBuffer l, final ByteBuffer i) {

		i.putInt((int)l.getLong());
	}

	public static final void LONG_TO_FLOAT(final ByteBuffer l, final ByteBuffer f) {

		f.putFloat((float)l.getLong());
	}

	public static final void LONG_TO_DOUBLE(final ByteBuffer l, final ByteBuffer f) {

		f.putDouble((float)l.getLong());
	}

	public static final void FLOAT_TO_BYTE(final ByteBuffer f, final ByteBuffer b) {

		b.put((byte)f.getFloat());
	}

	public static final void FLOAT_TO_SHORT(final ByteBuffer f, final ByteBuffer s) {

		s.putShort((short)f.getFloat());
	}

	public static final void FLOAT_TO_INT(final ByteBuffer f, final ByteBuffer i) {

		i.putInt((int)f.getFloat());
	}

	public static final void FLOAT_TO_LONG(final ByteBuffer f, final ByteBuffer l) {

		l.putLong((long)f.getFloat());
	}

	public static final void FLOAT_TO_DOUBLE(final ByteBuffer f, final ByteBuffer d) {

		d.putDouble((double)f.getFloat());
	}

	public static final void DOUBLE_TO_BYTE(final ByteBuffer d, final ByteBuffer b) {

		b.put((byte)d.getDouble());
	}

	public static final void DOUBLE_TO_SHORT(final ByteBuffer d, final ByteBuffer s) {

		s.putShort((short)d.getDouble());
	}

	public static final void DOUBLE_TO_INT(final ByteBuffer d, final ByteBuffer i) {

		i.putInt((int)d.getDouble());
	}

	public static final void DOUBLE_TO_LONG(final ByteBuffer d, final ByteBuffer l) {

		l.putLong((long)d.getDouble());
	}

	public static final void DOUBLE_TO_FLOAT(final ByteBuffer d, final ByteBuffer f) {

		f.putFloat((float)d.getDouble());
	}


}
