package org.janelia.saalfeldlab.n5.codec;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.IntFunction;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * De/serialize the {@link DataBlock#getData() data} contained in a {@code
 * DataBlock<T>} from/to a sequence of bytes.
 * <p>
 * Static fields {@code BYTE}, {@code SHORT_BIG_ENDIAN}, {@code
 * SHORT_LITTLE_ENDIAN}, etc. contain {@code DataCodec}s for all primitive array
 * types and big-endian / little-endian byte order.
 *
 * @param <T>
 * 		type of the data contained in the DataBlock
 */
public abstract class DataCodec<T> {

	public abstract ReadData serialize(T data) throws IOException;

	public abstract T deserialize(ReadData readData, int numElements) throws IOException;

	public int bytesPerElement() {
		return bytesPerElement;
	}

	public T createData(final int numElements) {
		return dataFactory.apply(numElements);
	}

	// ------------------- instances  --------------------
	//

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

	public static final DataCodec<String[]> STRING = new StringDataCodec();
	public static final DataCodec<byte[]> OBJECT = new ObjectDataCodec();

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
		public byte[] deserialize(final ReadData readData, int numElements) throws IOException {
			final byte[] data = createData((int)readData.length());
			new DataInputStream(readData.inputStream()).readFully(data);
			return data;
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
		public short[] deserialize(final ReadData readData, int numElements) throws IOException {

			final short[] data = createData(numElements);
			readData.toByteBuffer().order(order).asShortBuffer().get(data);
			return data;
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
		public int[] deserialize(final ReadData readData, int numElements) throws IOException {

			final int[] data = createData(numElements);
			final ByteBuffer byteBuffer = readData.toByteBuffer();
			final IntBuffer intBuffer = byteBuffer.order(order).asIntBuffer();
			intBuffer.get(data);
			return data;
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
		public long[] deserialize(final ReadData readData, int numElements) throws IOException {
			final long[] data = createData(numElements);
			readData.toByteBuffer().order(order).asLongBuffer().get(data);
			return data;
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
		public float[] deserialize(final ReadData readData, int numElements) throws IOException {
			final float[] data = createData(numElements);
			readData.toByteBuffer().order(order).asFloatBuffer().get(data);
			return data;
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
		public double[] deserialize(final ReadData readData, int numElements) throws IOException {

			final double[] data = createData(numElements);
			readData.toByteBuffer().order(order).asDoubleBuffer().get(data);
			return data;
		}
	}

	private static final class StringDataCodec extends DataCodec<String[]> {

		StringDataCodec() {
			super( -1, String[]::new);
		}

		private static final Charset ENCODING = StandardCharsets.UTF_8;
		private static final String NULLCHAR = "\0";


		@Override public ReadData serialize(String[] data) {
			final String flattenedArray = String.join(NULLCHAR, data) + NULLCHAR;
			return ReadData.from(flattenedArray.getBytes(ENCODING));
		}

		@Override public String[] deserialize(ReadData readData, int numElements) throws IOException {
			final byte[] serializedData = readData.allBytes();
			final String rawChars = new String(serializedData, ENCODING);
			return rawChars.split(NULLCHAR);
		}
	}

	private static final class ObjectDataCodec extends DataCodec<byte[]> {


		ObjectDataCodec() {
			super(-1, byte[]::new);
		}

		@Override public ReadData serialize(byte[] data) {

			return ReadData.from(data);
		}

		@Override public byte[] deserialize(ReadData readData, int numElements) throws IOException {
			final byte[] data = createData((int)readData.length());
			new DataInputStream(readData.inputStream()).readFully(data);
			return data;
		}
	}
}
