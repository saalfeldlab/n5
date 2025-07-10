package org.janelia.saalfeldlab.n5.readdata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.function.IntUnaryOperator;

import org.apache.commons.compress.utils.IOUtils;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.readdata.ReadData.OutputStreamOperator;
import org.junit.Test;

public class ReadDataTests {

	@Test
	public void testLazyReadData() throws IOException {

		final int N = 128;
		byte[] data = new byte[N];
		for( int i = 0; i < N; i++ )
			data[i] = (byte)i;

		final ReadData readData = ReadData.from(out -> {
			out.write(data);
		});
		assertTrue(readData instanceof LazyReadData);

		readDataTestHelper(readData, N);
		sliceTestHelper(readData, N);
	}

	@Test
	public void testByteArrayReadData() throws IOException {

		final int N = 128;
		byte[] data = new byte[N];
		for( int i = 0; i < N; i++ )
			data[i] = (byte)i;

		ReadData readData = ReadData.from(data).materialize();
		assertTrue(readData instanceof ByteArrayReadData);

		readDataTestHelper(readData, N);
		readDataTestEncodeHelper(readData, N);
		sliceTestHelper(readData, N);
	}

	@Test
	public void testInputStreamReadData() throws IOException {

		final int N = 128;
		final InputStream is = new InputStream() {
			int val = 0;
			@Override
			public int read() throws IOException {
				return val++;
			}
		};

		final ReadData readData = ReadData.from(is, N);
		readDataTestHelper(readData, N);
		sliceTestHelper(readData, N);
	}

	@Test
	public void testFileKvaReadData() throws IOException {

		int N = 128;
		byte[] data = new byte[N];
		for( int i = 0; i < N; i++ )
			data[i] = (byte)i;

		final File tmpF = File.createTempFile("test-file-splittable-data", ".bin");
		tmpF.deleteOnExit();
		try (FileOutputStream os = new FileOutputStream(tmpF)) {
			os.write(data);
		}

		final ReadData readData = new FileSystemKeyValueAccess(FileSystems.getDefault())
				.createReadData(tmpF.getAbsolutePath());

		assertEquals("file read data length", 128, readData.length());
		sliceTestHelper(readData, N);
	}

	@Test
	public void testConcatenatedReadData() throws IOException {

		final int N1 = 50;
		final int N2 = 30;
		final int N3 = 48;
		final int totalN = N1 + N2 + N3;

		byte[] data1 = new byte[N1];
		byte[] data2 = new byte[N2];
		byte[] data3 = new byte[N3];

		for (int i = 0; i < N1; i++)
			data1[i] = (byte) i;
		for (int i = 0; i < N2; i++)
			data2[i] = (byte) (i + N1);
		for (int i = 0; i < N3; i++)
			data3[i] = (byte) (i + N1 + N2);

		final ReadData part1 = ReadData.from(data1);
		final ReadData part2 = ReadData.from(data2);
		final ReadData part3 = ReadData.from(data3);

		final ConcatenatedReadData concatenated = new ConcatenatedReadData(part1, part2, part3);

		assertEquals("concatenated length", totalN, concatenated.length());

		byte[] allBytes = concatenated.allBytes();
		assertEquals("all bytes length", totalN, allBytes.length);
		for (int i = 0; i < totalN; i++) {
			assertEquals("byte at " + i, (byte) i, allBytes[i]);
		}

		readDataTestHelper(concatenated, totalN);
		sliceTestHelper(concatenated, totalN);

		ReadData slice = concatenated.slice(40, 60);
		assertEquals("slice length", 60, slice.length());
		byte[] sliceBytes = slice.allBytes();
		for (int i = 0; i < 60; i++) {
			assertEquals("slice byte at " + i, (byte) (i + 40), sliceBytes[i]);
		}

		ReadData crossBoundarySlice = concatenated.slice(45, 20);
		assertEquals("cross boundary slice length", 20, crossBoundarySlice.length());
		byte[] crossBoundaryBytes = crossBoundarySlice.allBytes();
		for (int i = 0; i < 20; i++) {
			assertEquals("cross boundary byte at " + i, (byte) (i + 45), crossBoundaryBytes[i]);
		}

		ReadData emptySlice = concatenated.slice(50, 0);
		assertEquals("empty slice length", 0, emptySlice.length());
		assertEquals("empty slice bytes", 0, emptySlice.allBytes().length);
	}

	@Test
	public void testConcatenatedReadDataWithUnknownLength() throws IOException {

		final int N1 = 50;
		final int N2 = 30;

		byte[] data1 = new byte[N1];
		byte[] data2 = new byte[N2];

		for (int i = 0; i < N1; i++)
			data1[i] = (byte) i;
		for (int i = 0; i < N2; i++)
			data2[i] = (byte) (i + N1);

		final ReadData part1 = ReadData.from(data1);
		final InputStream is = new InputStream() {
			int val = N1;
			int count = 0;
			@Override
			public int read() throws IOException {
				if (count++ < N2)
					return val++;
				return -1;
			}
		};
		final ReadData part2 = ReadData.from(is);

		final ConcatenatedReadData concatenated = new ConcatenatedReadData(part1, part2);

		assertEquals("concatenated with unknown length", -1, concatenated.length());

		byte[] allBytes = concatenated.allBytes();
		assertEquals("all bytes length", N1 + N2, allBytes.length);
		for (int i = 0; i < N1 + N2; i++) {
			assertEquals("byte at " + i, (byte) i, allBytes[i]);
		}
	}

	@Test
	public void testConcatenatedReadDataEdgeCases() throws IOException {

		final ReadData part1 = ReadData.from(new byte[]{1, 2, 3});
		final ReadData part2 = ReadData.from(new byte[]{4, 5, 6});
		final ConcatenatedReadData concatenated = new ConcatenatedReadData(part1, part2);

		assertThrows("negative offset", IndexOutOfBoundsException.class,
				() -> concatenated.slice(-1, 3));

		assertThrows("slice out of bounds", IndexOutOfBoundsException.class,
				() -> concatenated.slice(5, 3));

		ReadData materialized = concatenated.materialize();
		assertTrue("materialized is ByteArrayReadData", materialized instanceof ByteArrayReadData);
		assertArrayEquals("materialized content", new byte[]{1, 2, 3, 4, 5, 6}, materialized.allBytes());
	}

	@Test
	public void testConcatenatedReadDataWithEmptyList() throws IOException {
		// Test that ConcatenatedReadData behaves as an empty ReadData when created with empty list
		final ConcatenatedReadData emptyFromList = new ConcatenatedReadData(Arrays.asList());
		assertEquals("empty list length", 0, emptyFromList.length());
		assertArrayEquals("empty list allBytes", new byte[0], emptyFromList.allBytes());
		
		// Test varargs constructor with no arguments
		final ConcatenatedReadData emptyFromVarargs = new ConcatenatedReadData();
		assertEquals("empty varargs length", 0, emptyFromVarargs.length());
		assertArrayEquals("empty varargs allBytes", new byte[0], emptyFromVarargs.allBytes());
		
		// Test slicing empty ConcatenatedReadData
		final ReadData emptySlice = emptyFromList.slice(0, 0);
		assertEquals("empty slice length", 0, emptySlice.length());
		assertArrayEquals("empty slice allBytes", new byte[0], emptySlice.allBytes());
		
		// Test that slicing with non-zero offset throws exception
		assertThrows("slice with offset > 0", IndexOutOfBoundsException.class,
				() -> emptyFromList.slice(1, 0));
		
		// Test null list still throws exception
		assertThrows("null list", IllegalArgumentException.class,
				() -> new ConcatenatedReadData((java.util.List<ReadData>) null));
	}

	private void readDataTestHelper( ReadData readData, int N ) throws IOException {

		assertEquals("full length", N, readData.length());
	}

	private void readDataTestEncodeHelper( ReadData readData, int N ) throws IOException {

		final byte[] origCopy = new byte[N];
		IOUtils.readFully(readData.inputStream(), origCopy);

		final byte[] expected = Arrays.copyOf(origCopy, N);
		for( int i = 0; i < expected.length; i++)
			expected[i]+=2;

		final ReadData encoded = readData.encode(new ByteFun(x -> x+2));
		assertArrayEquals(expected, encoded.allBytes());

		final ReadData encodedTwice = encoded.encode(new ByteFun(x -> x-2));
		assertArrayEquals(origCopy, encodedTwice.allBytes());
	}

	private void sliceTestHelper( ReadData readData, int N ) throws IOException {

		assertEquals("length one", 1, readData.slice(9, 1).length());

		assertEquals("split length zero", 0, readData.slice(9, 0).length());
		assertEquals("split length zero allBytes", 0, readData.slice(9, 0).allBytes().length);

		ReadData limited = readData.limit(2);
		assertEquals(2, limited.length());

		ReadData unboundedLength = readData.slice(1, -1);
		assertEquals("unbounded length allBytes", N - 1, unboundedLength.allBytes().length);

		// slice may throw an exception if it knows its length and can detect out-of-bounds
		// otherwise the exception may be thrown on a read operation (e.g. allBytes)
		assertThrows("Out-of-range slice read", IndexOutOfBoundsException.class, () -> readData.slice(N-1, 3).allBytes());
		assertThrows("slice throws if offset too large", IndexOutOfBoundsException.class, () -> readData.slice(N, 0).allBytes());
		assertThrows("too large offset slice read", IndexOutOfBoundsException.class, () -> readData.slice(N-1, 3).allBytes());

		assertThrows("negative offset", IndexOutOfBoundsException.class, () -> readData.slice(-1, 1));
	}

	private static class ByteFun implements OutputStreamOperator {

		IntUnaryOperator fun;
		public ByteFun(IntUnaryOperator fun) {
			this.fun = fun;
		}

		@Override
		public OutputStream apply(OutputStream o) throws IOException {
			return new OutputStream() {
				@Override
				public void write(int b) throws IOException {
					o.write(fun.applyAsInt(b));
				}
			};
		}
	}

}
