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

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.ProxyInputStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

class ReadDataSplittableReadData implements SplittableReadData {

	private final SplittableReadData readData;
	private final long offset;
	private final long length;

	ReadDataSplittableReadData(final SplittableReadData readData, final long offset, final long length) {

		this.readData = readData;
		this.offset = offset;
		this.length = length;
	}

	@Override
	public long length() {

		return length;
	}

	@Override
	public InputStream inputStream() throws N5IOException {

		final InputStream offsetInputStream = new ProxyInputStream(readData.inputStream()) {

			private boolean firstRead = true;

			@Override protected void beforeRead(int n) throws IOException {

				if (firstRead) {
					inputStream().skip(offset);

					firstRead = false;
				}

				super.beforeRead(n);
			}
		};

		try {
			return BoundedInputStream.builder()
					.setInputStream(offsetInputStream)
					.setBufferSize((int)length)
					.get();
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}

	@Override
	public byte[] allBytes() throws N5IOException, IllegalStateException {

		final byte[] bytes = readData.allBytes();
		if (offset == 0 && bytes.length == length) {
			return bytes;
		} else {
			return Arrays.copyOfRange(bytes, (int)offset, (int)(offset + length));
		}
	}

	@Override
	public SplittableReadData materialize() {

		return this;
	}

	@Override
	public SplittableReadData slice(final long offset, final long length)  {

		return new ReadDataSplittableReadData(this, offset, length);
	}

	@Override
	public Pair<ReadData, ReadData> split(final long pivot) throws IOException {

		return ImmutablePair.of(slice(0, pivot), slice(offset + pivot, length - pivot));
	}
}
