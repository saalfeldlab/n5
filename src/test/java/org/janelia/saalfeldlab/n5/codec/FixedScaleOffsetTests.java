package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.DoubleStream;

import org.janelia.saalfeldlab.n5.DataType;
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

		final FixedScaleOffsetCodec double2Byte = new FixedScaleOffsetCodec(scale, offset, DataType.FLOAT64, DataType.INT8);
		AsTypeTests.testEncoding(double2Byte, bytes, encodedDoubles.array());
		AsTypeTests.testDecoding(double2Byte, encodedDoubles.array(), bytes);
	}

	@Test
	public void testLong2Short() throws IOException {

		final int N = 16;
		final ByteBuffer encodedLongs = ByteBuffer.allocate(Double.BYTES * N);
		final ByteBuffer encodedShorts = ByteBuffer.allocate(Short.BYTES * N);

		final long scale = 2;
		final long offset = 1;

		for (int i = 0; i < N; i++) {
			final long val = (scale * i + offset);
			encodedShorts.putShort((short)val);
			encodedLongs.putLong(i);
		}

		final byte[] shortBytes = encodedShorts.array();
		final byte[] longBytes = encodedLongs.array();

		final FixedScaleOffsetCodec long2short = new FixedScaleOffsetCodec(scale, offset, DataType.INT64, DataType.INT16);
		AsTypeTests.testEncoding(long2short, shortBytes, longBytes);
		AsTypeTests.testDecoding(long2short, longBytes, shortBytes);
	}



}
