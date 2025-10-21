/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.readdata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.DataCodec;

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
 * accessed (e.g., {@link #allBytes()}, {@link #writeTo(OutputStream)}), or
 * explicitly {@link #materialize() meterialized}.
 * <p>
 * {@code ReadData} can be {@link DataCodec#encode encoded} and {@link
 * DataCodec#decode decoded} by a {@link DataCodec}, which will also be lazy if
 * possible.
 */
public interface ReadData {

	/**
	 * Returns number of bytes in this {@link ReadData}, if known. Otherwise
	 * {@code -1}.
	 *
	 * @return number of bytes, if known, or -1
	 */
	default long length() {
		return -1;
	}

	/**
	 * Returns number of bytes in this {@link ReadData}. If the length is not
	 * currently know, this method may retrieve the length using I/O operations,
	 * {@link #materialize} this {@code ReadData}, or perform any other steps
	 * necessary to obtain the length.
	 *
	 * @return number of bytes
	 *
	 * @throws N5IOException
	 * 		if an I/O error occurs while trying to get the length
	 */
	long requireLength() throws N5IOException;
	// TODO: default: {materialize(); return length();}

	/**
	 * Returns a {@link ReadData} whose length is limited to the given value.
	 *
	 * @param length
	 *            the length of the resulting ReadData
	 * @return a length-limited ReadData
	 * @throws N5IOException
	 *             if an I/O error occurs while trying to get the length
	 */
	default ReadData limit(final long length) throws N5IOException {
		return slice(0, length);
	}

	/**
	 * Returns a new {@link ReadData} representing a slice, or subset
	 * of this ReadData.
	 *
	 * @param offset the offset relative to this ReadData
	 * @param length length of the returned ReadData
	 * @return a slice
	 * @throws N5IOException an exception
	 */
	default ReadData slice(final long offset, final long length) throws N5IOException {
		return materialize().slice(offset, length);
	}

	/**
	 * Returns a new {@link ReadData} representing a slice, or subset
	 * of this ReadData.
	 *
	 * @param range a range in this ReadData
	 * @return a slice
	 * @throws N5IOException an exception
	 */
	default ReadData slice(final Range range) throws N5IOException {
		return slice(range.offset(), range.length());
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
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 * @throws IllegalStateException
	 * 		if this method was already called once and cannot be called again.
	 */
	InputStream inputStream() throws N5IOException, IllegalStateException;

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
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 * @throws IllegalStateException
	 * 		if {@link #inputStream()} was already called once and cannot be called again.
	 */
	byte[] allBytes() throws N5IOException, IllegalStateException;

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
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 * @throws IllegalStateException
	 * 		if {@link #inputStream()} was already called once and cannot be called again.
	 */
	default ByteBuffer toByteBuffer() throws N5IOException, IllegalStateException {
		return ByteBuffer.wrap(allBytes());
	}

	/**
	 * Read the underlying data into a {@code byte[]} array, and return it as a {@code ReadData}.
	 * (If this {@code ReadData} is already in a {@code byte[]} array or {@code
	 * ByteBuffer}, just return {@code this}.)
	 * <p>
	 * The returned {@code ReadData} has a known {@link #length} and multiple
	 * {@link #inputStream InputStreams} can be opened on it.
	 * <p>
	 * <em>Implementation note: This should be preferably implemented to return
	 * {@code this}. For example, materialize into a new {@code byte[]}, {@code
	 * ReadData}, or similar and then delegate to this materialized version
	 * internally.</em>
	 *
	 * @return
	 * 		a materialized ReadData.
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	ReadData materialize() throws N5IOException;

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
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 * @throws IllegalStateException
	 * 		if {@link #inputStream()} was already called once and cannot be called again.
	 */
	default void writeTo(OutputStream outputStream) throws N5IOException, IllegalStateException {
		try {
			outputStream.write(allBytes());
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	/**
	 * Indicates that the given slices will be subsequently read.
	 * {@code ReadData} implementations (optionally) may take steps to prepare
	 * for these subsequent slices.
	 *
	 * @param ranges
	 * 		slice ranges to prefetch
	 *
	 * @throws N5IOException
	 * 		if any I/O error occurs
	 */
	default void prefetch(final Collection<? extends Range> ranges) throws N5IOException {
	}

	// ------------- Encoding / Decoding ----------------
	//

	/**
	 * Returns a new ReadData that uses the given {@code OutputStreamOperator} to
	 * encode this ReadData.
	 *
	 * @param encoder
	 * 		OutputStreamOperator to use for encoding
	 *
	 * @return encoded ReadData
	 */
	default ReadData encode(OutputStreamOperator encoder) {
		return new LazyReadData(this, encoder);
	}

	/**
	 * {@code OutputStreamOperator} is {@link #apply applied} to an {@code
	 * OutputStream} to transform it into another(e.g., compressed) {@code
	 * OutputStream}.
	 * <p>
	 * This is basically {@code UnaryOperator<OutputStream>}, but {@link #apply}
	 * throws {@code IOException}.
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
		return new ByteArrayReadData(data, offset, length);
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

	/**
	 * Returns an empty {@code ReadData}.
	 *
	 * @return an empty ReadData
	 */
	static ReadData empty() {
		return ByteArrayReadData.EMPTY;
	}

}
