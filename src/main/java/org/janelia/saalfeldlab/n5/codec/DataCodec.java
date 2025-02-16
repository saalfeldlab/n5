package org.janelia.saalfeldlab.n5.codec;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * TODO javadoc
 *  ... writes data contained in a DataBlock to byte[].
 *  ... versions for BIG_ENDIAN and LITTLE_ENDIAN byte order.
 *
 * @param <T>
 * 		type of the data contained in the DataBlock
 */
public abstract class DataCodec<T> {

	public static final DataCodec<byte[]>   BYTE              = new ByteDataCodec();
	public static final DataCodec<short[]>  SHORT_BIG_ENDIAN  = new ShortDataCodec(ByteOrder.BIG_ENDIAN);
	public static final DataCodec<int[]>    INT_BIG_ENDIAN    = new IntDataCodec(ByteOrder.BIG_ENDIAN);
	public static final DataCodec<long[]>   LONG_BIG_ENDIAN   = new LongDataCodec(ByteOrder.BIG_ENDIAN);
	public static final DataCodec<float[]>  FLOAT_BIG_ENDIAN  = new FloatDataCodec(ByteOrder.BIG_ENDIAN);
	public static final DataCodec<double[]> DOUBLE_BIG_ENDIAN = new DoubleDataCodec(ByteOrder.BIG_ENDIAN);

	public static final DataCodec<short[]>  SHORT_LITTLE_ENDIAN  = new ShortDataCodec(ByteOrder.LITTLE_ENDIAN);
	public static final DataCodec<int[]>    INT_LITTLE_ENDIAN    = new IntDataCodec(ByteOrder.LITTLE_ENDIAN);
	public static final DataCodec<long[]>   LONG_LITTLE_ENDIAN   = new LongDataCodec(ByteOrder.LITTLE_ENDIAN);
	public static final DataCodec<float[]>  FLOAT_LITTLE_ENDIAN  = new FloatDataCodec(ByteOrder.LITTLE_ENDIAN);
	public static final DataCodec<double[]> DOUBLE_LITTLE_ENDIAN = new DoubleDataCodec(ByteOrder.LITTLE_ENDIAN);

	public static DataCodec<short[]> SHORT(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? SHORT_BIG_ENDIAN : SHORT_LITTLE_ENDIAN;
	}

	public static DataCodec<int[]> INT(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? INT_BIG_ENDIAN : INT_LITTLE_ENDIAN;
	}

	public static DataCodec<long[]> LONG(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? LONG_BIG_ENDIAN : LONG_LITTLE_ENDIAN;
	}

	public static DataCodec<float[]> FLOAT(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? FLOAT_BIG_ENDIAN : FLOAT_LITTLE_ENDIAN;
	}

	public static DataCodec<double[]> DOUBLE(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? DOUBLE_BIG_ENDIAN : DOUBLE_LITTLE_ENDIAN;
	}

	public abstract ReadData serialize(T data) throws IOException;

	public abstract void deserialize(ReadData readData, T data) throws IOException;

	public int bytesPerElement() {
		return bytesPerElement;
	}

	public T createData(final int numElements) {
		return dataFactory.apply(numElements);
	}

	// ---------------- implementations  -----------------
	//

	private final int bytesPerElement;
	private final IntFunction<T> dataFactory;

	private DataCodec(int bytesPerElement, IntFunction<T> dataFactory) {
		this.bytesPerElement = bytesPerElement;
		this.dataFactory = dataFactory;
	}

	private static final class ByteDataCodec extends DataCodec<byte[]> {

		private ByteDataCodec() {
			super(Byte.BYTES, byte[]::new);
		}

		@Override
		public ReadData serialize(final byte[] data) {
			return ReadData.from(data);
		}

		@Override
		public void deserialize(final ReadData readData, final byte[] data) throws IOException {
			new DataInputStream(readData.inputStream()).readFully(data);
		}
	}

	private static final class ShortDataCodec extends DataCodec<short[]> {

		private final ByteOrder order;

		ShortDataCodec(ByteOrder order) {
			super(Short.BYTES, short[]::new);
			this.order = order;
		}

		@Override
		public ReadData serialize(final short[] data) {
			final ByteBuffer serialized = ByteBuffer.allocate(Short.BYTES * data.length);
			serialized.order(order).asShortBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public void deserialize(final ReadData readData, final short[] data) throws IOException {
			readData.toByteBuffer().order(order).asShortBuffer().get(data);
		}
	}

	private static final class IntDataCodec extends DataCodec<int[]> {

		private final ByteOrder order;

		IntDataCodec(ByteOrder order) {
			super(Integer.BYTES, int[]::new);
			this.order = order;
		}

		@Override
		public ReadData serialize(final int[] data) {
			final ByteBuffer serialized = ByteBuffer.allocate(Integer.BYTES * data.length);
			serialized.order(order).asIntBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public void deserialize(final ReadData readData, final int[] data) throws IOException {
			readData.toByteBuffer().order(order).asIntBuffer().get(data);
		}
	}

	private static final class LongDataCodec extends DataCodec<long[]> {

		private final ByteOrder order;

		LongDataCodec(ByteOrder order) {
			super(Long.BYTES, long[]::new);
			this.order = order;
		}

		@Override
		public ReadData serialize(final long[] data) {
			final ByteBuffer serialized = ByteBuffer.allocate(Long.BYTES * data.length);
			serialized.order(order).asLongBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public void deserialize(final ReadData readData, final long[] data) throws IOException {
			readData.toByteBuffer().order(order).asLongBuffer().get(data);
		}
	}

	private static final class FloatDataCodec extends DataCodec<float[]> {

		private final ByteOrder order;

		FloatDataCodec(ByteOrder order) {
			super(Float.BYTES, float[]::new);
			this.order = order;
		}

		@Override
		public ReadData serialize(final float[] data) {
			final ByteBuffer serialized = ByteBuffer.allocate(Float.BYTES * data.length);
			serialized.order(order).asFloatBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public void deserialize(final ReadData readData, final float[] data) throws IOException {
			readData.toByteBuffer().order(order).asFloatBuffer().get(data);
		}
	}

	private static final class DoubleDataCodec extends DataCodec<double[]> {

		private final ByteOrder order;

		DoubleDataCodec(ByteOrder order) {
			super(Double.BYTES, double[]::new);
			this.order = order;
		}

		@Override
		public ReadData serialize(final double[] data) {
			final ByteBuffer serialized = ByteBuffer.allocate(Double.BYTES * data.length);
			serialized.order(order).asDoubleBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public void deserialize(final ReadData readData, final double[] data) throws IOException {
			readData.toByteBuffer().order(order).asDoubleBuffer().get(data);
		}
	}
}
