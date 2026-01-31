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
package org.janelia.saalfeldlab.n5.shard;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.kva.VolatileReadData;

public class TestPositionValueAccess implements PositionValueAccess {

	private final Map<Key, byte[]> map = new HashMap<>();

	@Override
	public VolatileReadData get(final long[] key) {
		final byte[] bytes = map.get(new Key(key));
		return bytes == null ? null : new ClosableWrapper(ReadData.from(bytes));
	}

	public void set(final long[] key, final ReadData data) {

		if (data == null) {
			map.remove(new Key(key));
			return;
		}

		final byte[] bytes = data.allBytes();
		map.put(new Key(key), bytes);
	}

	@Override
	public boolean exists(long[] key) throws N5IOException {
		return map.containsKey(new Key(key));
	}

	@Override
	public boolean remove(final long[] key) throws N5IOException {
		return map.remove(new Key(key)) != null;
	}

	private static class Key {

		private final long[] data;

		Key(long[] data) {

			this.data = data;
		}

		@Override
		public final boolean equals(final Object o) {

			if (!(o instanceof Key)) {
				return false;
			}
			final Key key = (Key)o;
			return Arrays.equals(data, key.data);
		}

		@Override
		public int hashCode() {

			return Arrays.hashCode(data);
		}
	}

	private static class ClosableWrapper implements VolatileReadData {

		private final ReadData delegate;

		ClosableWrapper(final ReadData delegate) {
			this.delegate = delegate;
		}

		@Override
		public long length() {
			return delegate.length();
		}

		@Override
		public long requireLength() throws N5IOException {
			return delegate.requireLength();
		}

		@Override
		public ReadData limit(final long length) throws N5IOException {
			return delegate.limit(length);
		}

		@Override
		public ReadData slice(final long offset, final long length) throws N5IOException {
			return delegate.slice(offset, length);
		}

		@Override
		public ReadData slice(final Range range) throws N5IOException {
			return delegate.slice(range);
		}

		@Override
		public InputStream inputStream() throws N5IOException, IllegalStateException {
			return delegate.inputStream();
		}

		@Override
		public byte[] allBytes() throws N5IOException, IllegalStateException {
			return delegate.allBytes();
		}

		@Override
		public ByteBuffer toByteBuffer() throws N5IOException, IllegalStateException {
			return delegate.toByteBuffer();
		}

		@Override
		public ReadData materialize() throws N5IOException {
			delegate.materialize();
			return this;
		}

		@Override
		public void writeTo(final OutputStream outputStream) throws N5IOException, IllegalStateException {
			delegate.writeTo(outputStream);
		}

		@Override
		public void prefetch(final Collection<? extends Range> ranges) throws N5IOException {
			delegate.prefetch(ranges);
		}

		@Override
		public ReadData encode(final OutputStreamOperator encoder) {
			return delegate.encode(encoder);
		}

		@Override
		public void close() throws N5IOException {
		}
	}
}
