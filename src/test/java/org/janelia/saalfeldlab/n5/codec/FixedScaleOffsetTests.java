package org.janelia.saalfeldlab.n5.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.stream.DoubleStream;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.FixedScaleOffsetCodec;
import org.junit.Test;

public class FixedScaleOffsetTests {

	@Test
	public void testDouble2Byte() throws IOException {

		final int N = 16;
		final double[] doubles = DoubleStream.iterate(0.0, x -> x + 1).limit(N).toArray();
		final ByteBuffer encodedDoubles = ByteBuffer.allocate(Double.BYTES * N);
		final byte[] bytes = new byte[N];

		final double scale = 2;
		final double offset = 1;

		for (int i = 0; i < N; i++) {
			final double val = (scale * doubles[i] + offset);
			bytes[i] = (byte)val;
			encodedDoubles.putDouble(i);
		}

		final FixedScaleOffsetCodec double2Byte = new FixedScaleOffsetCodec(scale, offset, DataType.FLOAT64, DataType.UINT8);
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
		final byte[] convertedArr = outputStream.toByteArray();
		assertArrayEquals( expected, outputStream.toByteArray());
		encodedStream.close();
	}

}
