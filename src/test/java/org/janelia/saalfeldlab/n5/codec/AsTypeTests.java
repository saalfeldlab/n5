package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.junit.Test;

public class AsTypeTests {

	@Test
	public void testInt2Byte() throws IOException {

		final int N = 16;
		final ByteBuffer intsAsBuffer = ByteBuffer.allocate(Integer.BYTES * N);
		final byte[] encodedBytes = new byte[N];
		for (int i = 0; i < N; i++) {
			intsAsBuffer.putInt(i);
			encodedBytes[i] = (byte)i;
		}

		final byte[] decodedInts = intsAsBuffer.array();
		testEncodingAndDecoding(new AsTypeCodec(DataType.INT32, DataType.INT8), encodedBytes, decodedInts);
		testEncodingAndDecoding(new AsTypeCodec(DataType.INT8, DataType.INT32), decodedInts, encodedBytes);
	}

	@Test
	public void testDouble2Byte() throws IOException {

		final int N = 16;
		final ByteBuffer doublesAsBuffer = ByteBuffer.allocate(Double.BYTES * N);
		final byte[] encodedBytes = new byte[N];
		for (int i = 0; i < N; i++) {
			doublesAsBuffer.putDouble(i);
			encodedBytes[i] = (byte)i;
		}
		final byte[] decodedDoubles = doublesAsBuffer.array();

		testEncodingAndDecoding(new AsTypeCodec(DataType.FLOAT64, DataType.INT8), encodedBytes, decodedDoubles);
		testEncodingAndDecoding(new AsTypeCodec(DataType.INT8, DataType.FLOAT64), decodedDoubles, encodedBytes);
	}

	public static void testEncodingAndDecoding(Codec.BytesCodec codec, byte[] encodedBytes, byte[] decodedBytes) throws IOException {

		testEncoding(codec, encodedBytes, decodedBytes);
		testDecoding(codec, decodedBytes, encodedBytes);
	}

	public static void testDecoding(final Codec.BytesCodec codec, final byte[] expected, final byte[] input) throws IOException {

		final ReadData result = codec.decode(ReadData.from(input));
		assertArrayEquals(expected, result.allBytes());
	}

	public static void testEncoding(final Codec.BytesCodec codec, final byte[] expected, final byte[] data) throws IOException {

		final byte[] encodedData = codec.encode(ReadData.from(data)).allBytes();
		assertArrayEquals(expected, encodedData);
	}

}
