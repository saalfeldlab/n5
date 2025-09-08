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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.ProxyOutputStream;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

class LazyReadData implements ReadData {

	LazyReadData(final OutputStreamWriter writer) {
		this.writer = writer;
	}

	/**
	 * Construct a {@code LazyReadData} that uses the given {@code OutputStreamOperator} to
	 * encode the given {@code ReadData}.
	 *
	 * @param data
	 * 		the ReadData to encode
	 * @param encoder
	 * 		OutputStreamOperator to use for encoding
	 */
	LazyReadData(final ReadData data, final OutputStreamOperator encoder) {
		this(outputStream -> {
			try (final OutputStream deflater = encoder.apply(interceptClose.apply(outputStream))) {
				data.writeTo(deflater);
			}
		});
	}

	private final OutputStreamWriter writer;

	private ByteArrayReadData bytes;

	private long length = -1;

	@Override
	public ReadData materialize() throws N5IOException {
		if (bytes == null) {
			try {
				final ByteArrayOutputStream baos = new ByteArrayOutputStream(8192);
				writer.writeTo(baos);
				bytes = new ByteArrayReadData(baos.toByteArray());
			} catch (IOException e) {
				throw new N5IOException(e);
			}
		}
		return this;
	}

	@Override
	public long length() {
		return (bytes != null) ? bytes.length() : length;
	}

	// TODO: remove? Should this be the default implementation?
	// TODO: could just always return materialize().length()?
	@Override
	public long requireLength() throws N5IOException {
		long l = length();
		if ( l >= 0 ) {
			return l;
		} else {
			return materialize().length();
		}
	}

	@Override
	public ReadData slice(final long offset, final long length) throws N5IOException {
		materialize();
		return bytes.slice(offset, length);
	}

	@Override
	public InputStream inputStream() throws N5IOException, IllegalStateException {
		materialize();
		return bytes.inputStream();
	}

	@Override
	public byte[] allBytes() throws N5IOException, IllegalStateException {
		materialize();
		return bytes.allBytes();
	}

	@Override
	public void writeTo(final OutputStream outputStream) throws N5IOException, IllegalStateException {
		try {
			if (bytes != null) {
				outputStream.write(bytes.allBytes());
			} else {
				final CountingOutputStream cos = new CountingOutputStream(outputStream);
				writer.writeTo(cos);
				length = cos.getByteCount();
			}
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	/**
	 * {@code UnaryOperator} that wraps {@code OutputStream} to intercept {@code
	 * close()} and call {@code flush()} instead
	 */
	private static final OutputStreamOperator interceptClose = o -> new ProxyOutputStream(o) {

		@Override
		public void close() throws IOException {
			out.flush();
		}
	};
}
