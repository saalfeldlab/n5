package org.janelia.saalfeldlab.n5.readdata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.janelia.saalfeldlab.n5.KeyValueAccess;

/**
 * An abstraction over {@code byte[]} data.
 * <p>
 * The data may come from a {@code byte[]} array, a {@code ByteBuffer}, an
 * {@code InputStream}, a {@code KeyValueAccess}.
 * <p>
 * {@code ReadData} instances can be created via one of the static {@link #from}
 * methods. For example, use {@link #from(InputStream, int)} to wrap an {@code
 * InputStream}.
 * <p>
 * {@code ReadData} may be lazy-loaded. For example, for {@code InputStream} and
 * {@code KeyValueAccess} sources, loading is deferred until the data is
 * accessed (e.g., {@link #allBytes()}, {@link #writeTo(OutputStream)}).
 * <p>
 * {@code ReadData} can be {@code encoded} and {@code decoded} with a {@code
 * Codec}, which will also be lazy if possible.
 */
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
	 * Return the contained data as a {@code byte[]} array.
	 * <p>
	 * This may use {@link #inputStream()} to read the data.
	 * Because repeatedly calling {@link #inputStream()} may not work,
	 * <ol>
	 * <li>this method may fail with {@code IllegalStateException} if {@code inputStream()} was already called</li>
	 * <li>subsequent {@code inputStream()} calls may fail with {@code IllegalStateException}</li>
	 * </ol>
	 *
	 * @return all contained data as a byte[] array
	 *
	 * @throws IOException
	 * 		if any I/O error occurs
	 * @throws IllegalStateException
	 * 		if {@link #inputStream()} was already called once and cannot be called again.
	 */
	byte[] allBytes() throws IOException, IllegalStateException;

	/**
	 * Return the contained data as a {@code ByteBuffer}.
	 * <p>
	 * This may use {@link #inputStream()} to read the data.
	 * Because repeatedly calling {@link #inputStream()} may not work,
	 * <ol>
	 * <li>this method may fail with {@code IllegalStateException} if {@code inputStream()} was already called</li>
	 * <li>subsequent {@code inputStream()} calls may fail with {@code IllegalStateException}</li>
	 * </ol>
	 * The byte order of the returned {@code ByteBuffer} is {@code BIG_ENDIAN}.
	 *
	 * @return all contained data as a ByteBuffer
	 *
	 * @throws IOException
	 * 		if any I/O error occurs
	 * @throws IllegalStateException
	 * 		if {@link #inputStream()} was already called once and cannot be called again.
	 */
	default ByteBuffer toByteBuffer() throws IOException, IllegalStateException {
		return ByteBuffer.wrap(allBytes());
	}

	/**
	 * Read the underlying data into a {@code byte[]} array, and return it as a {@code ReadData}.
	 * (If this {@code ReadData} is already in a {@code byte[]} array or {@code
	 * ByteBuffer}, just return {@code this}.)
	 * <p>
	 * The returned {@code ReadData} has a known {@link #length} and multiple
	 * {@link #inputStream InputStreams} can be opened on it.
	 */
	ReadData materialize() throws IOException;

	/**
	 * Write the contained data into an {@code OutputStream}.
	 * <p>
	 * This may use {@link #inputStream()} to read the data.
	 * Because repeatedly calling {@link #inputStream()} may not work,
	 * <ol>
	 * <li>this method may fail with {@code IllegalStateException} if {@code inputStream()} was already called</li>
	 * <li>subsequent {@code inputStream()} calls may fail with {@code IllegalStateException}</li>
	 * </ol>
	 *
	 * @param outputStream
	 * 		destination to write to
	 *
	 * @throws IOException
	 * 		if any I/O error occurs
	 * @throws IllegalStateException
	 * 		if {@link #inputStream()} was already called once and cannot be called again.
	 */
	default void writeTo(OutputStream outputStream) throws IOException, IllegalStateException {
		outputStream.write(allBytes());
	}

	// ------------- Encoding / Decoding ----------------
	//

	/**
	 * Returns a new ReadData that uses the given {@code OutputStreamEncoder} to
	 * encode this ReadData.
	 *
	 * @param encoder
	 * 		OutputStreamEncoder to use for encoding
	 *
	 * @return encoded ReadData
	 */
	default ReadData encode(OutputStreamOperator encoder) {
		return new LazyReadData(this, encoder);
	}

	/**
	 * Like {@code UnaryOperator<OutputStream>}, but {@code apply} throws {@code IOException}.
	 */
	@FunctionalInterface
	interface OutputStreamOperator {

		OutputStream apply(OutputStream o) throws IOException;

	}

	// --------------- Factory Methods ------------------
	//

	/**
	 * Create a new {@code ReadData} that loads lazily from {@code inputStream}
	 * and {@link #length() reports} the given {@code length}.
	 * <p>
	 * No effort is made to ensure that the {@code inputStream} in fact contains
	 * exactly {@code length} bytes.
	 *
	 * @param inputStream
	 * 		InputStream to read from
	 * @param length
	 * 		reported length of the ReadData
	 *
	 * @return a new ReadData
	 */
	static ReadData from(final InputStream inputStream, final int length) {
		return new InputStreamReadData(inputStream, length);
	}

	/**
	 * Create a new {@code ReadData} that loads lazily from {@code inputStream}
	 * and reports {@link #length() length() == -1} (i.e., unknown length).
	 *
	 * @param inputStream
	 * 		InputStream to read from
	 *
	 * @return a new ReadData
	 */
	static ReadData from(final InputStream inputStream) {
		return from(inputStream, -1);
	}

	/**
	 * Create a new {@code ReadData} that loads lazily from {@code normalPath}
	 * in {@code keyValueAccess}. The returned ReadData reports {@link #length()
	 * length() == -1} (i.e., unknown length).
	 *
	 * @param keyValueAccess
	 * 		KeyValueAccess to read from
	 * @param normalPath
	 * 		path in the {@code keyValueAccess} to read from
	 *
	 * @return a new ReadData
	 */
	static ReadData from(final KeyValueAccess keyValueAccess, final String normalPath) {
		return new KeyValueAccessReadData(keyValueAccess, normalPath);
	}

	/**
	 * Create a new {@code ReadData} that wraps the specified portion of a
	 * {@code byte[]} array.
	 *
	 * @param data
	 * 		array containing the data
	 * @param offset
	 * 		start offset of the ReadData in the data array
	 * @param length
	 * 		length of the ReadData (in bytes)
	 *
	 * @return a new ReadData
	 */
	static ReadData from(final byte[] data, final int offset, final int length) {
		return new ByteArraySplittableReadData(data, offset, length);
	}

	/**
	 * Create a new {@code ReadData} that wraps the given {@code byte[]} array.
	 *
	 * @param data
	 * 		array containing the data
	 *
	 * @return a new ReadData
	 */
	static ReadData from(final byte[] data) {
		return from(data, 0, data.length);
	}

	/**
	 * Create a new {@code ReadData} that wraps the given {@code ByteBuffer}.
	 *
	 * @param data
	 * 		buffer containing the data
	 *
	 * @return a new ReadData
	 */
	static ReadData from(final ByteBuffer data) {
		if (data.hasArray()) {
			return from(data.array(), 0, data.limit());
		} else {
			throw new UnsupportedOperationException("TODO. Direct ByteBuffer not supported yet.");
		}
	}

	@FunctionalInterface
	interface OutputStreamWriter {
		void writeTo(OutputStream outputStream) throws IOException, IllegalStateException;
	}

	/**
	 * Create a new {@code ReadData} that is lazily generated by the given {@link OutputStreamWriter}.
	 *
	 * @param generator
	 * 		generates the data
	 *
	 * @return a new ReadData
	 */
	static ReadData from(OutputStreamWriter generator) {
		return new LazyReadData(generator);
	}
}
