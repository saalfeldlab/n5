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
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * De/serialize the {@link DataBlock#getData() data} contained in a {@code
 * DataBlock<T>} from/to a sequence of bytes.
 * <p>
 * Static fields {@code BYTE}, {@code SHORT_BIG_ENDIAN}, {@code
 * SHORT_LITTLE_ENDIAN}, etc. contain {@code FlatArrayCodec}s for all primitive
 * array types and big-endian / little-endian byte order.
 *
 * @param <T>
 * 		type of the data contained in the DataBlock
 */
public abstract class FlatArrayCodec<T> {

	public abstract ReadData encode(T data) throws N5IOException;

	public abstract T decode(ReadData readData, int numElements) throws N5IOException;

	public int bytesPerElement() {
		return bytesPerElement;
	}

	public T newArray(final int numElements) {
		return dataFactory.apply(numElements);
	}

	// ------------------- instances  --------------------
	//

	public static final FlatArrayCodec<byte[]>   BYTE              = new ByteArrayCodec();
	public static final FlatArrayCodec<short[]>  SHORT_BIG_ENDIAN  = new ShortArrayCodec(ByteOrder.BIG_ENDIAN);
	public static final FlatArrayCodec<int[]>    INT_BIG_ENDIAN    = new IntArrayCodec(ByteOrder.BIG_ENDIAN);
	public static final FlatArrayCodec<long[]>   LONG_BIG_ENDIAN   = new LongArrayCodec(ByteOrder.BIG_ENDIAN);
	public static final FlatArrayCodec<float[]>  FLOAT_BIG_ENDIAN  = new FloatArrayCodec(ByteOrder.BIG_ENDIAN);
	public static final FlatArrayCodec<double[]> DOUBLE_BIG_ENDIAN = new DoubleArrayCodec(ByteOrder.BIG_ENDIAN);

	public static final FlatArrayCodec<short[]>  SHORT_LITTLE_ENDIAN  = new ShortArrayCodec(ByteOrder.LITTLE_ENDIAN);
	public static final FlatArrayCodec<int[]>    INT_LITTLE_ENDIAN    = new IntArrayCodec(ByteOrder.LITTLE_ENDIAN);
	public static final FlatArrayCodec<long[]>   LONG_LITTLE_ENDIAN   = new LongArrayCodec(ByteOrder.LITTLE_ENDIAN);
	public static final FlatArrayCodec<float[]>  FLOAT_LITTLE_ENDIAN  = new FloatArrayCodec(ByteOrder.LITTLE_ENDIAN);
	public static final FlatArrayCodec<double[]> DOUBLE_LITTLE_ENDIAN = new DoubleArrayCodec(ByteOrder.LITTLE_ENDIAN);

	public static final FlatArrayCodec<String[]> STRING = new N5StringArrayCodec();
	public static final FlatArrayCodec<String[]> ZARR_STRING = new ZarrStringArrayCodec();
	public static final FlatArrayCodec<byte[]>   OBJECT = new ObjectArrayCodec();

	public static FlatArrayCodec<short[]> SHORT(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? SHORT_BIG_ENDIAN : SHORT_LITTLE_ENDIAN;
	}

	public static FlatArrayCodec<int[]> INT(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? INT_BIG_ENDIAN : INT_LITTLE_ENDIAN;
	}

	public static FlatArrayCodec<long[]> LONG(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? LONG_BIG_ENDIAN : LONG_LITTLE_ENDIAN;
	}

	public static FlatArrayCodec<float[]> FLOAT(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? FLOAT_BIG_ENDIAN : FLOAT_LITTLE_ENDIAN;
	}

	public static FlatArrayCodec<double[]> DOUBLE(ByteOrder order) {
		return order == ByteOrder.BIG_ENDIAN ? DOUBLE_BIG_ENDIAN : DOUBLE_LITTLE_ENDIAN;
	}

	// ---------------- implementations  -----------------
	//

	private final int bytesPerElement;
	private final IntFunction<T> dataFactory;

	private FlatArrayCodec(int bytesPerElement, IntFunction<T> dataFactory) {
		this.bytesPerElement = bytesPerElement;
		this.dataFactory = dataFactory;
	}

	private static final class ByteArrayCodec extends FlatArrayCodec<byte[]> {

		private ByteArrayCodec() {
			super(Byte.BYTES, byte[]::new);
		}

		@Override
		public ReadData encode(final byte[] data) {
			return ReadData.from(data);
		}

		@Override
		public byte[] decode(final ReadData readData, int numElements) throws N5IOException {
			final byte[] data = newArray(numElements);
			try {
				new DataInputStream(readData.inputStream()).readFully(data);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
			return data;
		}
	}

	private static final class ShortArrayCodec extends FlatArrayCodec<short[]> {

		private final ByteOrder order;

		ShortArrayCodec(ByteOrder order) {
			super(Short.BYTES, short[]::new);
			this.order = order;
		}

		@Override
		public ReadData encode(final short[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Short.BYTES * data.length);
			serialized.order(order).asShortBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public short[] decode(final ReadData readData, int numElements) throws N5IOException {
			final short[] data = newArray(numElements);
			readData.toByteBuffer().order(order).asShortBuffer().get(data);
			return data;
		}
	}

	private static final class IntArrayCodec extends FlatArrayCodec<int[]> {

		private final ByteOrder order;

		IntArrayCodec(ByteOrder order) {
			super(Integer.BYTES, int[]::new);
			this.order = order;
		}

		@Override
		public ReadData encode(final int[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Integer.BYTES * data.length);
			serialized.order(order).asIntBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public int[] decode(final ReadData readData, int numElements) throws N5IOException {
			final int[] data = newArray(numElements);
			final ByteBuffer byteBuffer = readData.toByteBuffer();
			final IntBuffer intBuffer = byteBuffer.order(order).asIntBuffer();
			intBuffer.get(data);
			return data;
		}
	}

	private static final class LongArrayCodec extends FlatArrayCodec<long[]> {

		private final ByteOrder order;

		LongArrayCodec(ByteOrder order) {
			super(Long.BYTES, long[]::new);
			this.order = order;
		}

		@Override
		public ReadData encode(final long[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Long.BYTES * data.length);
			serialized.order(order).asLongBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public long[] decode(final ReadData readData, int numElements) throws N5IOException {
			final long[] data = newArray(numElements);
			readData.toByteBuffer().order(order).asLongBuffer().get(data);
			return data;
		}
	}

	private static final class FloatArrayCodec extends FlatArrayCodec<float[]> {

		private final ByteOrder order;

		FloatArrayCodec(ByteOrder order) {
			super(Float.BYTES, float[]::new);
			this.order = order;
		}

		@Override
		public ReadData encode(final float[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Float.BYTES * data.length);
			serialized.order(order).asFloatBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public float[] decode(final ReadData readData, int numElements) throws N5IOException {
			final float[] data = newArray(numElements);
			readData.toByteBuffer().order(order).asFloatBuffer().get(data);
			return data;
		}
	}

	private static final class DoubleArrayCodec extends FlatArrayCodec<double[]> {

		private final ByteOrder order;

		DoubleArrayCodec(ByteOrder order) {
			super(Double.BYTES, double[]::new);
			this.order = order;
		}

		@Override
		public ReadData encode(final double[] data) throws N5IOException {
			final ByteBuffer serialized = ByteBuffer.allocate(Double.BYTES * data.length);
			serialized.order(order).asDoubleBuffer().put(data);
			return ReadData.from(serialized);
		}

		@Override
		public double[] decode(final ReadData readData, int numElements) throws N5IOException {
			final double[] data = newArray(numElements);
			readData.toByteBuffer().order(order).asDoubleBuffer().get(data);
			return data;
		}
	}

	private static final class N5StringArrayCodec extends FlatArrayCodec<String[]> {

		private static final Charset ENCODING = StandardCharsets.UTF_8;
		private static final String NULLCHAR = "\0";

		N5StringArrayCodec() {
			super( -1, String[]::new);
		}

		@Override
		public ReadData encode(String[] data) throws N5IOException {
			final String flattenedArray = String.join(NULLCHAR, data) + NULLCHAR;
			return ReadData.from(flattenedArray.getBytes(ENCODING));
		}

		@Override
		public String[] decode(ReadData readData, int numElements) throws N5IOException {
			final byte[] serializedData = readData.allBytes();
			final String rawChars = new String(serializedData, ENCODING);
			return rawChars.split(NULLCHAR);
		}
	}

	private static final class ZarrStringArrayCodec extends FlatArrayCodec<String[]> {

		private static final Charset ENCODING = StandardCharsets.UTF_8;

		ZarrStringArrayCodec() {
			super( -1, String[]::new);
		}

		@Override
		public ReadData encode(String[] data) throws N5IOException {
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
		public String[] decode(ReadData readData, int numElements) throws N5IOException {
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

	private static final class ObjectArrayCodec extends FlatArrayCodec<byte[]> {

		ObjectArrayCodec() {
			super(-1, byte[]::new);
		}

		@Override
		public ReadData encode(byte[] data) throws N5IOException {
			return ReadData.from(data);
		}

		@Override
		public byte[] decode(ReadData readData, int numElements) throws N5IOException {
			final byte[] data = newArray(numElements);
			try {
				new DataInputStream(readData.inputStream()).readFully(data);
			} catch (IOException e) {
				throw new N5IOException(e);
			}
			return data;
		}
	}
}
