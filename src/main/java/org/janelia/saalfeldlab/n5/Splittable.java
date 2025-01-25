package org.janelia.saalfeldlab.n5;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class Splittable {


	public interface ReadData {

		/**
		 * Returns number of bytes in this {@link ReadData}, if known. Otherwise
		 * {@code -1}.
		 *
		 * @return number of bytes, if known, or -1
		 *
		 * @throws IOException
		 * 		if an I/O error occurs while trying to get the length
		 */
		default long length() throws IOException {
			return -1;
		}

		/**
		 * Open a {@code InputStream} on this data.
		 * <p>
		 * Repeatedly calling this method may or may not work, depending on how
		 * the underlying data is stored. For example, if the underlying data is
		 * stored as a {@code byte[]} array, multiple streams can be opened. If
		 * the underlying data is just an {@code InputStream} then this will be
		 * returned on the first call.
		 *
		 * @return an InputStream on this data
		 *
		 * @throws IOException
		 * 		if any I/O error occurs
		 * @throws IllegalStateException
		 * 		if this method was already called once and cannot be called again.
		 */
		InputStream inputStream() throws IOException, IllegalStateException;

		/**
		 * If this {@code ReadData} is a {@code SplittableReadData}, just returns {@code this}.
		 * <p>
		 * Otherwise, if the underlying data is an {@code InputStream}, all data is read and
		 * wrapped as a {@code ByteArraySplittableReadData}.
		 * <p>
		 * The returned {@code SplittableReadData} has a known {@link #length}
		 * and multiple {@link #inputStream}s can be opened on it.
		 */
		SplittableReadData splittable() throws IOException;
	}

	public interface SplittableReadData extends ReadData {

		ReadData split(final long offset, final long length) throws IOException;
	}

	public static class ByteArraySplittableReadData implements SplittableReadData {

		private final byte[] data;
		private final int offset;
		private final int length;

		ByteArraySplittableReadData(final byte[] data, final int offset, final int length) {
			this.data = data;
			this.offset = offset;
			this.length = length;
		}

		@Override
		public long length() {
			return length;
		}

		@Override
		public InputStream inputStream() throws IOException {
			return new ByteArrayInputStream(data, offset, length);
		}

		@Override
		public SplittableReadData splittable() throws IOException {
			return this;
		}

		@Override
		public SplittableReadData split(final long offset, final long length) throws IOException {
			if (offset < 0 || offset > this.length || length < 0) {
				throw new IndexOutOfBoundsException();
			}
			final int o = this.offset + (int) offset;
			final int l = Math.min((int) length, this.length - o);
			return new ByteArraySplittableReadData(data, o, l);
		}
	}

	public static class InputStreamReadData implements ReadData {

		private final InputStream inputStream;
		private final int length;
		private ByteArraySplittableReadData bytes;

		InputStreamReadData(final InputStream inputStream, final int length) {
			this.inputStream = inputStream;
			this.length = length;
		}

		InputStreamReadData(final InputStream inputStream) {
			this(inputStream, -1);
		}

		@Override
		public long length() {
			return length;
		}

		@Override
		public SplittableReadData splittable() throws IOException {
			if (bytes == null) {
				final byte[] data;
				if (length >= 0) {
					data = new byte[length];
					new DataInputStream(inputStream()).readFully(data);
				} else {
					data = Java9StreamMethods.readAllBytes(inputStream());
				}
				bytes = new ByteArraySplittableReadData(data, 0, data.length);
			}
			return bytes;
		}

		private boolean inputStreamCalled = false;

		@Override
		public InputStream inputStream() throws IOException {
			if ( inputStreamCalled ) {
				throw new IOException("InputStream() already called");
			} else {
				inputStreamCalled = true;
				return inputStream;
			}
		}
	}

	public static class KeyValueAccessReadData implements ReadData {

		private final KeyValueAccess keyValueAccess;
		private final String normalPath;

		KeyValueAccessReadData(final KeyValueAccess keyValueAccess, final String normalPath) {
			this.keyValueAccess = keyValueAccess;
			this.normalPath = normalPath;
		}

		@Override
		public InputStream inputStream() throws IOException, IllegalStateException {
			final LockedChannel lockedChannel = keyValueAccess.lockForReading(normalPath);
			return lockedChannel.newInputStream();
			// TODO bind close() of the InputStream to close of the lockedChannel
		}

		@Override
		public SplittableReadData splittable() throws IOException {
			return null;
		}
	}


}
