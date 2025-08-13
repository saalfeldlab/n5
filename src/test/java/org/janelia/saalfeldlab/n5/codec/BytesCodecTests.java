package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.function.IntUnaryOperator;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.ReadData.OutputStreamOperator;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;
import org.junit.BeforeClass;
import org.junit.Test;

public class BytesCodecTests {

	static Random random;

	@BeforeClass
	public static void setup() {
		random = new Random(7777);
	}

	@Test
	public void testEncodeDecodeBytes() {

		// Create a BitShiftBytesCodec with shift value
		final BitShiftBytesCodec originalCodec = new BitShiftBytesCodec(3);

		// Test encode/decode roundtrip
		final byte[] testData = new byte[12];
		random.nextBytes(testData);

		final ReadData original = ReadData.from(testData);
		final ReadData encoded = originalCodec.encode(original);
		final ReadData decoded = originalCodec.decode(encoded);

		final byte[] result = decoded.allBytes();
		assertEquals("Length should match", testData.length, result.length);
		assertArrayEquals("encoded-decoded bytes should match original", testData, result);
	}

	@Test
	public void concatenatedBytesCodecTest() throws IOException {

		int N = 16;
		ReadData data = ReadData.from( new InputStream() {
			@Override
			public int read() throws IOException {
				return Math.abs(random.nextInt()) % 32;
			}
		}, N ).materialize();

		final byte[] bytes = data.allBytes();
		final byte[] expected = new byte[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			expected[i] = (byte)(2 * bytes[i] + 3);
		}

		final BytesCodec a = new ByteFunctionCodec(x -> 2 * x, x -> x / 2);
		final BytesCodec b = new ByteFunctionCodec(x -> x + 3, x -> x - 3 );
		final ConcatenatedBytesCodec ab = new ConcatenatedBytesCodec(new BytesCodec[]{a, b});

		final ReadData encodedData = ab.encode(data).materialize();
		assertArrayEquals(expected, encodedData.allBytes());

		final ReadData decodedData = ab.decode(encodedData).materialize();
		assertArrayEquals(bytes, decodedData.allBytes());
	}

	public static class ByteFunctionCodec implements BytesCodec {

		IntUnaryOperator encoder;
		IntUnaryOperator decoder;

		public ByteFunctionCodec( IntUnaryOperator encoder, IntUnaryOperator decoder ) {
			this.encoder = encoder;
			this.decoder = decoder;
		}

		@Override
		public String getType() {
			return "byteFunction";
		}

		public ReadData decode(ReadData data) {
			return data.encode(new ByteFun(decoder));
		}

		public ReadData encode(ReadData data) {
			return data.encode(new ByteFun(encoder));
		}
	}

	private static class ByteFun implements OutputStreamOperator {

		IntUnaryOperator fun;
		public ByteFun(IntUnaryOperator fun) {
			this.fun = fun;
		}

		@Override
		public OutputStream apply(OutputStream o) {
			return new OutputStream() {
				@Override
				public void write(int b) throws IOException {
					o.write(fun.applyAsInt(b));
				}
			};
		}
	}

	@NameConfig.Name(BitShiftBytesCodec.TYPE)
	public static class BitShiftBytesCodec implements BytesCodec {

		private static final String TYPE = "bitshift";

		@NameConfig.Parameter
		private int shift;

		public BitShiftBytesCodec() {

			shift = 0;
		}

		public BitShiftBytesCodec(int shift) {

			this.shift = shift;
		}

		@Override
		public String getType() {

			return TYPE;
		}

		@Override
		public ReadData decode(ReadData readData) throws N5IOException {

			if (shift == 0) {
				return readData;
			}

			final byte[] data = readData.allBytes();
			final byte[] decoded = new byte[data.length];

			// Apply inverse bit shift (right rotate) to decode
			for (int i = 0; i < data.length; i++) {
				int b = data[i] & 0xFF;
				decoded[i] = (byte)((b >>> shift) | (b << (8 - shift)));
			}

			return ReadData.from(decoded);
		}

		@Override
		public ReadData encode(ReadData readData) throws N5IOException {

			if (shift == 0) {
				return readData;
			}

			byte[] data = readData.allBytes();
			byte[] encoded = new byte[data.length];

			// Apply bit shift (left rotate) to encode
			for (int i = 0; i < data.length; i++) {
				int b = data[i] & 0xFF;
				encoded[i] = (byte)((b << shift) | (b >>> (8 - shift)));
			}
			return ReadData.from(encoded);
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}
			if (obj == null || getClass() != obj.getClass()) {
				return false;
			}
			BitShiftBytesCodec other = (BitShiftBytesCodec)obj;
			return shift == other.shift;
		}

		@Override
		public int hashCode() {

			return Integer.hashCode(shift);
		}

	}

}
