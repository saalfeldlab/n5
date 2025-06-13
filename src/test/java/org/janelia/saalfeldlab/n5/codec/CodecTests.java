package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.function.IntUnaryOperator;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.ReadData.OutputStreamOperator;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.junit.BeforeClass;
import org.junit.Test;

public class CodecTests {

	static Random random;

	@BeforeClass
	public static void setup() {
		random = new Random(7777);
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
		final ConcatenatedBytesCodec ab = new ConcatenatedBytesCodec(a, b);

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

}
