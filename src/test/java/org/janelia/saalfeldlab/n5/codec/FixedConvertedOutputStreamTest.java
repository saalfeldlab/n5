package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.codec.AsTypeCodec;
import org.janelia.saalfeldlab.n5.codec.FixedLengthConvertedOutputStream;
import org.junit.Test;

public class FixedConvertedOutputStreamTest {

	@Test
	public void testLengthOne() throws IOException
	{
		final int N = 2;
		final byte expected = 5;
		final byte expectedPlusOne = 6;
		final byte[] expectedData = new byte[N];
		Arrays.fill(expectedData, expected);

		final byte[] expectedPlusOneData = new byte[N];
		Arrays.fill(expectedPlusOneData, expectedPlusOne);

		final ByteArrayOutputStream outId = new ByteArrayOutputStream(N);
		final FixedLengthConvertedOutputStream convertedId = new FixedLengthConvertedOutputStream(1, 1,
				AsTypeCodec.IDENTITY_ONE,
				outId);

		convertedId.write(expectedData);
		convertedId.flush();
		convertedId.close();

		assertArrayEquals(expectedData, outId.toByteArray());


		final ByteArrayOutputStream outPlusOne = new ByteArrayOutputStream(N);
		final FixedLengthConvertedOutputStream convertedPlusOne = new FixedLengthConvertedOutputStream(1, 1,
				(x, y) -> {
					y.put((byte)(x.get() + 1));
				},
				outPlusOne);

		convertedPlusOne.write(expectedData);
		convertedPlusOne.close();
		assertArrayEquals(expectedPlusOneData, outPlusOne.toByteArray());
	}

	@Test
	public void testIntToByte() throws IOException
	{

		final int N = 16;
		final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES * N);
		IntStream.range(0, N).forEach( x -> {
			buf.putInt(x);
		});

		final ByteBuffer expected = ByteBuffer.allocate(N);
		IntStream.range(0, N).forEach( x -> {
			expected.put((byte)x);
		});

		final ByteArrayOutputStream outStream = new ByteArrayOutputStream(N);
		final FixedLengthConvertedOutputStream intToByte = new FixedLengthConvertedOutputStream(
				4, 1,
				AsTypeCodec.INT_TO_BYTE,
				outStream);

		intToByte.write(buf.array());
		intToByte.close();

		System.out.println(Arrays.toString(buf.array()));
		System.out.println(Arrays.toString(expected.array()));
		System.out.println(Arrays.toString(outStream.toByteArray()));
//
//		assertArrayEquals(expected.array(), outStream.toByteArray());
	}
//
//	@Test
//	public void testByteToInt() throws IOException
//	{
//
//		final int N = 16;
//		final byte[] data = new byte[16];
//		for( int i = 0; i < N; i++ )
//			data[i] = (byte)i;
//
//		FixedLengthConvertedInputStream byteToInt = new FixedLengthConvertedInputStream(
//				1, 4,
//				(x, y) -> {
//					y[0] = 0;  // the setting to zero is not strictly necessary in this case
//					y[1] = 0;
//					y[2] = 0;
//					y[3] = x[0];
//				},
//				new ByteArrayInputStream(data));
//
//		final DataInputStream dataStream = new DataInputStream(byteToInt);
//		for( int i = 0; i < N; i++ )
//			assertEquals(i, dataStream.readInt());
//
//		dataStream.close();
//		byteToInt.close();
//	}

}
