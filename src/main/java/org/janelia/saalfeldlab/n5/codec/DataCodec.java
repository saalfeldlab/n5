/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
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
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
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

	public abstract ReadData serialize(T data) throws N5IOException;

	public abstract T deserialize(ReadData readData, int numElements) throws N5IOException;

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

	public static final DataCodec<String[]> STRING = new N5StringDataCodec();
	public static final DataCodec<String[]> ZARR_STRING = new ZarrStringDataCodec();

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

	public static DataCodec<String[]> ZARR_UNICODE(int nChars, ByteOrder order) {
		return new ZarrUnicodeStringDataCodec(nChars, order);
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
		public byte[] deserialize(final ReadData readData, int numElements) throws N5IOException {
			final byte[] data = createData(numElements);
			try {
				new DataInputStream(readData.inputStream()).readFully(data);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
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
		public ReadData serialize(final short[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Short.BYTES * data.length);
			serialized.order(order).asShortBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public short[] deserialize(final ReadData readData, int numElements) throws N5IOException {
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
		public ReadData serialize(final int[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Integer.BYTES * data.length);
			serialized.order(order).asIntBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public int[] deserialize(final ReadData readData, int numElements) throws N5IOException {
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
		public ReadData serialize(final long[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Long.BYTES * data.length);
			serialized.order(order).asLongBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public long[] deserialize(final ReadData readData, int numElements) throws N5IOException {
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
		public ReadData serialize(final float[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Float.BYTES * data.length);
			serialized.order(order).asFloatBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public float[] deserialize(final ReadData readData, int numElements) throws N5IOException {
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
		public ReadData serialize(final double[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Double.BYTES * data.length);
			serialized.order(order).asDoubleBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public double[] deserialize(final ReadData readData, int numElements) throws N5IOException {
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

		@Override
		public ReadData serialize(String[] data) throws N5IOException {
			final String flattenedArray = String.join(NULLCHAR, data) + NULLCHAR;
			return ReadData.from(flattenedArray.getBytes(ENCODING));
		}

		@Override
		public String[] deserialize(ReadData readData, int numElements) throws N5IOException {
			final byte[] serializedData = readData.allBytes();
			final String rawChars = new String(serializedData, ENCODING);
			return rawChars.split(NULLCHAR);
		}
	}

	private static final class ZarrStringDataCodec extends DataCodec<String[]> {

		private static final Charset ENCODING = StandardCharsets.UTF_8;

		ZarrStringDataCodec() {
			super( -1, String[]::new);
		}

		@Override
		public ReadData serialize(String[] data) throws N5IOException {
			final int N = data.length;
			final byte[][] encodedStrings = Arrays.stream(data).map(str -> str.getBytes(ENCODING)).toArray(byte[][]::new);
			final int[] lengths = Arrays.stream(encodedStrings).mapToInt(a -> a.length).toArray();
			final int totalLength = Arrays.stream(lengths).sum();
			final ByteBuffer buf = ByteBuffer.wrap(new byte[totalLength + 4 * N + 4]);
			buf.order(ByteOrder.LITTLE_ENDIAN);
			buf.putInt(N);
			for (int i = 0; i < N; ++i) {
				buf.putInt(lengths[i]);
				buf.put(encodedStrings[i]);
			}
			return ReadData.from(buf.array());
		}

		@Override
		public String[] deserialize(ReadData readData, int numElements) throws N5IOException {
			final ByteBuffer serialized = readData.toByteBuffer();
			serialized.order(ByteOrder.LITTLE_ENDIAN);

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

	private static final class ZarrUnicodeStringDataCodec extends DataCodec<String[]> {

		private static final char NULL_CHAR = '\0';
		private static final int BYTES_PER_CHAR = 4;

		private final Charset charset;

		ZarrUnicodeStringDataCodec(int nChar, ByteOrder order) {
			super(nChar * BYTES_PER_CHAR, String[]::new);
			if (order == ByteOrder.BIG_ENDIAN)
				charset = Charset.forName("UTF-32BE");
			else
				charset = Charset.forName("UTF-32LE");
		}

		@Override
		public ReadData serialize(String[] data) throws N5IOException {

			if( data.length == 0 )
				return ReadData.empty();

			final int N = data.length;
			final int maxLength = Arrays.stream(data).mapToInt( String::length).max().getAsInt();
			final int fixedEncodedStringSize = maxLength * BYTES_PER_CHAR;
			final int totalSize = N * fixedEncodedStringSize;
			final ByteBuffer buf = ByteBuffer.allocate(totalSize);
			int pos = 0;
			for( int i = 0; i < N; i++ ) {
				buf.position(pos);
				buf.put(charset.encode(data[i]));
				pos += fixedEncodedStringSize;
			}
			return ReadData.from(buf.array());
		}

		@Override
		public String[] deserialize(ReadData readData, int numElements) throws N5IOException {

			if (readData.length() % numElements != 0) {
				throw new RuntimeException(String.format("Data of length (%d) bytes is not a multiple of numElements (%d)",
						readData.length(), numElements));
			}

			final int bytesPerString = (int)readData.length() / numElements;
			final ByteBuffer serialized = readData.toByteBuffer();
			int pos = 0;
			final String[] out = new String[numElements];
			for (int i = 0; i < numElements; i++) {
				serialized.position(pos);
				serialized.limit(pos + bytesPerString);
				out[i] = removeTrailingNullChars(charset.decode(serialized).toString());
				pos += bytesPerString;
			}
			return out;
		}

		private static String removeTrailingNullChars(String str) {
			int idx = str.indexOf(NULL_CHAR);
			return idx < 0 ? str : str.substring(0, idx);
		}
	}

	private static final class ObjectDataCodec extends DataCodec<byte[]> {

		ObjectDataCodec() {
			super(-1, byte[]::new);
		}

		@Override
		public ReadData serialize(byte[] data) throws N5IOException {
			return ReadData.from(data);
		}

		@Override
		public byte[] deserialize(ReadData readData, int numElements) throws N5IOException {
			final byte[] data = createData(numElements);
			try {
				new DataInputStream(readData.inputStream()).readFully(data);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
			return data;
		}
	}
}
