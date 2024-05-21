package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.AsTypeCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.junit.Test;

public class AsTypeTests {

	@Test
	public void testInt2Byte() throws IOException {

		final int N = 16;
		final int[] ints = IntStream.rangeClosed(0, N).toArray();
		final ByteBuffer encodedInts = ByteBuffer.allocate(Integer.BYTES * N);
		final byte[] bytes = new byte[N];
		for (int i = 0; i < N; i++) {

			bytes[i] = (byte)ints[i];
			encodedInts.putInt(ints[i]);
		}

		final AsTypeCodec int2Byte = new AsTypeCodec(DataType.UINT32, DataType.UINT8);
		testEncoding( int2Byte, bytes, encodedInts.array());
		testDecoding( int2Byte, encodedInts.array(), bytes);

		final AsTypeCodec byte2Int = new AsTypeCodec(DataType.UINT8, DataType.UINT32);
		testEncoding( byte2Int, encodedInts.array(), bytes);
		testDecoding( byte2Int, bytes, encodedInts.array());
	}

	@Test
	public void testDouble2Byte() throws IOException {

		final int N = 16;
		final double[] doubles = new double[N];
		final byte[] bytes = new byte[N];
		final ByteBuffer encodedDoubles = ByteBuffer.allocate(Double.BYTES * N);
		for (int i = 0; i < N; i++) {
			doubles[i] = i;
			encodedDoubles.putDouble(doubles[i]);

			bytes[i] = (byte)i;
		}

		final AsTypeCodec double2Byte = new AsTypeCodec(DataType.FLOAT64, DataType.INT8);
		testEncoding(double2Byte, bytes, encodedDoubles.array());
		testDecoding(double2Byte, encodedDoubles.array(), bytes);
	}

	protected static void testDecoding( final Codec codec, final byte[] expected, final byte[] input ) throws IOException
	{
		final InputStream result = codec.decode(new ByteArrayInputStream(input));
		for (int i = 0; i < expected.length; i++)
			assertEquals(expected[i], (byte)result.read());
	}

	protected static void testEncoding( final Codec codec, final byte[] expected, final byte[] data ) throws IOException
	{

		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(expected.length);
		final OutputStream encodedStream = codec.encode(outputStream);
		encodedStream.write(data);
		encodedStream.flush();
		assertArrayEquals(expected, outputStream.toByteArray());
		encodedStream.close();
	}

}
