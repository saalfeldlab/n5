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
import org.apache.commons.lang3.tuple.Pair;
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
		splittableReadDataTestHelper(readData.materialize(), N, 5);
	}

	@Test
	public void testByteArrayReadData() throws IOException {

		final int N = 128;
		byte[] data = new byte[N];
		for( int i = 0; i < N; i++ )
			data[i] = (byte)i;

		SplittableReadData readData = ReadData.from(data).materialize();
		assertTrue(readData instanceof ByteArraySplittableReadData);

		readDataTestHelper(readData, N);
		readDataTestEncodeHelper(readData, N);
		splittableReadDataTestHelper(readData, N, 5);
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

	private void splittableReadDataTestHelper( SplittableReadData readData, int N, int pivot ) throws IOException {

		assertEquals("length one", 1, readData.slice(9, 1).length());

		assertEquals("split length zero", 0, readData.slice(9, 0).length());
		assertEquals("split length zero allBytes", 0, readData.slice(9, 0).allBytes().length);

		ReadData limited = readData.limit(2);
		assertEquals(2, limited.length());

		ReadData splitOutOfRange = readData.slice(N-1, 3);
		assertEquals("Out-of-range split truncates", 1, splitOutOfRange.length());
		assertEquals("Out-of-range split truncates allBytes", 1, splitOutOfRange.allBytes().length);
		
		ReadData unboundedLength = readData.slice(1, Integer.MAX_VALUE);
		assertEquals("unbounded length", N - 1, unboundedLength.length());
		assertEquals("unbounded length allBytes", N - 1, unboundedLength.allBytes().length);
		
		assertThrows("negative offset", IndexOutOfBoundsException.class, () -> readData.slice(-1, 1));
		assertThrows("negative length", IndexOutOfBoundsException.class, () -> readData.slice(0, -1));
		assertThrows("too large offset", IndexOutOfBoundsException.class, () -> readData.slice(N, 1));

		final Pair<ReadData, ReadData> split = readData.split(pivot);
		final ReadData first = split.getLeft();
		final ReadData last = split.getRight();

		assertEquals(pivot, first.length());
		assertEquals(0, first.allBytes()[0]);

		assertEquals(N-pivot, last.length());
		assertEquals(pivot, last.allBytes()[0]);
	}

	@Test
	public void testInputStreamReadData() throws IOException {

		final int N = 128;
		byte[] data = new byte[N];
		for( int i = 0; i < N; i++ )
			data[i] = (byte)i;

		final InputStream is = new InputStream() {
			int val = 0;
			@Override
			public int read() throws IOException {
				return val++;
			}
		};

		final ReadData readData = ReadData.from(is, N);
		readDataTestHelper(readData, N);
		splittableReadDataTestHelper(readData.materialize(), N, 5);
	}

	@Test
	public void testFileSplittableReadData() throws IOException {

		int N = 128;
		byte[] data = new byte[N];
		for( int i = 0; i < N; i++ )
			data[i] = (byte)i;

		final File tmpF = File.createTempFile("test-file-splittable-data", ".bin");
		tmpF.deleteOnExit();
		try (FileOutputStream os = new FileOutputStream(tmpF)) {
			os.write(data);
			os.close();
		}

		final FileSplittableReadData readData = new FileSystemKeyValueAccess(FileSystems.getDefault())
				.createReadData(tmpF.getAbsolutePath());
		assertEquals("file read data length", 128, readData.length());
		splittableReadDataTestHelper(readData.materialize(), N, 5);
	}
	
	private class ByteFun implements OutputStreamOperator {

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
