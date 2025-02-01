package org.janelia.saalfeldlab.n5.readdata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.janelia.saalfeldlab.n5.BytesCodec;

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
	 * If this {@code ReadData} is a {@code SplittableReadData}, just returns {@code this}.
	 * <p>
	 * Otherwise, if the underlying data is an {@code InputStream}, all data is read and
	 * wrapped as a {@code ByteArraySplittableReadData}.
	 * <p>
	 * The returned {@code SplittableReadData} has a known {@link #length}
	 * and multiple {@link #inputStream}s can be opened on it.
	 */
	SplittableReadData splittable() throws IOException;


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
	 * @param outputStream destination to write to
	 * @throws IOException
	 * 		if any I/O error occurs
	 * @throws IllegalStateException
	 * 		if {@link #inputStream()} was already called once and cannot be called again.
	 */
	default void writeTo(OutputStream outputStream) throws IOException, IllegalStateException {
		outputStream.write(allBytes());
	}

	// TODO: WIP, exploring API options...
	default ReadData encode(BytesCodec codec) throws IOException {
		return codec.encode(this);
	}

	// TODO: WIP, exploring API options...
	default ReadData decode(BytesCodec codec) throws IOException {
		return decode(codec, -1);
	}

	// TODO: WIP, exploring API options...
	default ReadData decode(BytesCodec codec, int decodedLength) throws IOException {
		return codec.decode(this, decodedLength);
	}
}
