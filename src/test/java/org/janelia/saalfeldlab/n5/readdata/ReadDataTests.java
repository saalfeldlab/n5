package org.janelia.saalfeldlab.n5.readdata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

public class ReadDataTests {

	@Test
	public void testByteArrayReadData() throws IOException {

		final int N = 128;
		byte[] data = new byte[N];
		for( int i = 0; i < N; i++ )
			data[i] = (byte)i;

		SplittableReadData readData = ReadData.from(data).splittable();
		assertTrue(readData instanceof ByteArraySplittableReadData);

		assertEquals("full length", N, readData.length());
		assertEquals("length one", 1, readData.split(9, 1).length());

		assertEquals("split length zero", 0, readData.split(9, 0).length());
		assertEquals("split length zero allBytes", 0, readData.split(9, 0).allBytes().length);

		ReadData splitOutOfRange = readData.split(N-1, 3);
		assertEquals("Out-of-range split truncates", 1, splitOutOfRange.length());
		assertEquals("Out-of-range split truncates allBytes", 1, splitOutOfRange.allBytes().length);

		/*
		 * Using Long.MAX_VALUE breaks ByteArraySplittableReadData.split
		 * because the line:
		 * 		final int l = Math.min((int) length, this.length - o);
		 * returns -1
		 * 
		 * do we care to address this?
		 */
//		ReadData unboundedLength = readData.split(1, Long.MAX_VALUE);

		ReadData unboundedLength = readData.split(1, Integer.MAX_VALUE);
		assertEquals("unbounded length", N - 1, unboundedLength.length());
		assertEquals("unbounded length allBytes", N - 1, unboundedLength.allBytes().length);

		assertThrows("negative offset", IndexOutOfBoundsException.class, () -> readData.split(-1, 1));
		assertThrows("negative length", IndexOutOfBoundsException.class, () -> readData.split(0, -1));
		assertThrows("too large offset", IndexOutOfBoundsException.class, () -> readData.split(N, 1));
	}
}
