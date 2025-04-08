package org.janelia.saalfeldlab.n5.codec;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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

	public static final DataCodec<byte[]>   BYTE              		= new ByteDataCodec();
	public static final DataCodec<short[]>  SHORT_BIG_ENDIAN  		= new ShortDataCodec(ByteOrder.BIG_ENDIAN);
	public static final DataCodec<int[]>    INT_BIG_ENDIAN    		= new IntDataCodec(ByteOrder.BIG_ENDIAN);
	public static final DataCodec<long[]>   LONG_BIG_ENDIAN   		= new LongDataCodec(ByteOrder.BIG_ENDIAN);
	public static final DataCodec<float[]>  FLOAT_BIG_ENDIAN  		= new FloatDataCodec(ByteOrder.BIG_ENDIAN);
	public static final DataCodec<double[]> DOUBLE_BIG_ENDIAN 		= new DoubleDataCodec(ByteOrder.BIG_ENDIAN);
	public static final DataCodec<String[]> ZARR_STRING_BIG_STRING	= new ZarrStringDataCodec(ByteOrder.BIG_ENDIAN);

	public static final DataCodec<short[]>  SHORT_LITTLE_ENDIAN  		= new ShortDataCodec(ByteOrder.LITTLE_ENDIAN);
	public static final DataCodec<int[]>    INT_LITTLE_ENDIAN    		= new IntDataCodec(ByteOrder.LITTLE_ENDIAN);
	public static final DataCodec<long[]>   LONG_LITTLE_ENDIAN   		= new LongDataCodec(ByteOrder.LITTLE_ENDIAN);
	public static final DataCodec<float[]>  FLOAT_LITTLE_ENDIAN  		= new FloatDataCodec(ByteOrder.LITTLE_ENDIAN);
	public static final DataCodec<double[]> DOUBLE_LITTLE_ENDIAN		= new DoubleDataCodec(ByteOrder.LITTLE_ENDIAN);
	public static final DataCodec<String[]> ZARR_STRING_LITTLE_STRING 	= new ZarrStringDataCodec(ByteOrder.LITTLE_ENDIAN);

	public static final DataCodec<String[]> STRING = new N5StringDataCodec();
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
			final byte[] data = createData(numElements);
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

	private static final class N5StringDataCodec extends DataCodec<String[]> {

		private static final Charset ENCODING = StandardCharsets.UTF_8;
		private static final String NULLCHAR = "\0";

		N5StringDataCodec() {
			super( -1, String[]::new);
		}

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

	private static final class ZarrStringDataCodec extends DataCodec<String[]> {

		private final ByteOrder order;
		private static final Charset ENCODING = StandardCharsets.UTF_8;

		ZarrStringDataCodec(ByteOrder order) {
			super( -1, String[]::new);
			this.order = order;
		}

		@Override public ReadData serialize(String[] data) {

			final int N = data.length;
			final byte[][] encodedStrings = Arrays.stream(data).map(str -> str.getBytes(ENCODING)).toArray(byte[][]::new);
			final int[] lengths = Arrays.stream(encodedStrings).mapToInt(a -> a.length).toArray();
			final int totalLength = Arrays.stream(lengths).sum();
			final ByteBuffer buf = ByteBuffer.wrap(new byte[totalLength + 4 * N + 4]);
			buf.order(order);
			buf.putInt(N);
			for (int i = 0; i < N; ++i) {
				buf.putInt(lengths[i]);
				buf.put(encodedStrings[i]);
			}
			return ReadData.from(buf.array());
		}

		@Override public String[] deserialize(ReadData readData, int numElements) throws IOException {

			final ByteBuffer serialized =  readData.toByteBuffer();
			serialized.order(order);

			// sanity check to avoid out of memory errors
			if (serialized.limit() < 4)
				throw new RuntimeException("Corrupt buffer, data seems truncated.");

			final int n = serialized.getInt();
			if (serialized.limit() < n)
				throw new RuntimeException("Corrupt buffer, data seems truncated.");

			final String[] actualData = new String[n];
			for (int i = 0; i < n; ++i) {
				final int length = serialized.getInt();
				final byte[] encodedString = new byte[length];
				serialized.get(encodedString);
				actualData[i] = new String(encodedString, ENCODING);
			}
			return actualData;
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
			final byte[] data = createData(numElements);
			new DataInputStream(readData.inputStream()).readFully(data);
			return data;
		}
	}
}
