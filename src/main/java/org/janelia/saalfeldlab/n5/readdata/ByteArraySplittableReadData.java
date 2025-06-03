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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

class ByteArraySplittableReadData implements ReadData {

	private final byte[] data;
	private final int offset;
	private final int length;

	ByteArraySplittableReadData(final byte[] data) {

		this(data, 0, data.length);
	}

	ByteArraySplittableReadData(final byte[] data, final int offset, final int length) {

		this.data = data;
		this.offset = offset;

		if( length < 0 )
			this.length = data.length - offset;
		else
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
	public byte[] allBytes() {

		// Arrays.copyOfRange pads with zeros, so explicitly check for out-of-bounds
		// allow offset = 0 and length = 0 to return empty array
		if ( (offset > 0 && offset >= data.length) || offset + length > data.length)
			throw new IndexOutOfBoundsException();

		if (offset == 0 && data.length == length) {
			return data;
		} else {
			return Arrays.copyOfRange(data, offset, offset + length);
		}
	}

	@Override
	public ReadData materialize() throws IOException {
		return this;
	}

	@Override
	public ReadData slice(final long offset, final long length) throws IOException {

		if (offset < 0 || offset >= this.length )
			throw new IndexOutOfBoundsException();

		final int o = this.offset + (int)offset;
		return new ByteArraySplittableReadData(data, o, (int)length);
	}

	@Override
	public Pair<ReadData, ReadData> split(final long pivot) throws IOException {

		return ImmutablePair.of(slice(0, pivot), slice(offset + pivot, length - pivot));
	}
}
