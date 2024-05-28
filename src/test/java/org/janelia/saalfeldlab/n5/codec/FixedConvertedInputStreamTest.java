package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.Test;

public class FixedConvertedInputStreamTest {

	@Test
	public void testLengthOne() throws IOException
	{

		final byte expected = 5;
		final byte[] data = new byte[32];
		Arrays.fill(data, expected);

		final FixedLengthConvertedInputStream convertedId = new FixedLengthConvertedInputStream(1, 1,
				AsTypeCodec::IDENTITY_ONE,
				new ByteArrayInputStream(data));

		final FixedLengthConvertedInputStream convertedPlusOne = new FixedLengthConvertedInputStream(1, 1,
				(x, y) -> {
					y.put((byte)(x.get() + 1));
				},
				new ByteArrayInputStream(data));

		for (int i = 0; i < 32; i++) {
			assertEquals(expected, convertedId.read());
			assertEquals(expected + 1, convertedPlusOne.read());
		}

		convertedId.close();
		convertedPlusOne.close();
	}

	@Test
	public void testIntToByte() throws IOException
	{

		final int N = 16;
		final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES * N);
		IntStream.range(0, N).forEach( x -> {
			buf.putInt(x);
		});

		final byte[] data = buf.array();
		final FixedLengthConvertedInputStream intToByte = new FixedLengthConvertedInputStream(
				4, 1,
				AsTypeCodec::INT_TO_BYTE,
				new ByteArrayInputStream(data));

		for( int i = 0; i < N; i++ )
			assertEquals((byte)i, intToByte.read());

		intToByte.close();
	}

	@Test
	public void testByteToInt() throws IOException
	{

		final int N = 16;
		final byte[] data = new byte[16];
		for( int i = 0; i < N; i++ )
			data[i] = (byte)i;

		final FixedLengthConvertedInputStream byteToInt = new FixedLengthConvertedInputStream(
				1, 4, AsTypeCodec::BYTE_TO_INT,
				new ByteArrayInputStream(data));

		final DataInputStream dataStream = new DataInputStream(byteToInt);
		for( int i = 0; i < N; i++ )
			assertEquals(i, dataStream.readInt());

		dataStream.close();
		byteToInt.close();
	}

}
