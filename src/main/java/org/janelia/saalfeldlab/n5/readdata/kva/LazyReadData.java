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
package org.janelia.saalfeldlab.n5.readdata.kva;

import java.io.IOException;
import java.io.InputStream;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A {@link ReadData} implementation that is backed by {@link LazyRead} object.
 */
public class LazyReadData implements VolatileReadData {

    private final LazyRead lazyRead;
    private ReadData materialized;
    private final long offset;
    private long length;

	public LazyReadData(final LazyRead lazyRead) {
        this(lazyRead, 0, -1);
    }

    private LazyReadData(final LazyRead lazyRead, final long offset, final long length) {
        this.lazyRead = lazyRead;
        this.offset = offset;
        this.length = length;
    }

    @Override
	public ReadData materialize() throws N5IOException {
		if (materialized == null) {
			materialized = lazyRead.materialize(offset, length);
			length = materialized.length();
		}
		return this;
	}

	/**
	 * Returns a {@link ReadData} whose length is limited to the given value.
	 * <p>
	 * This implementation defers a material read operation if allowed
	 * by the {@link LazyRead}.
	 *
	 * @param length
	 *            the length of the resulting ReadData
	 * @return a length-limited ReadData
	 * @throws N5IOException
	 *             if an I/O error occurs while trying to get the length
	 */
    @Override
    public ReadData slice(final long offset, final long length) {
        if (offset < 0)
            throw new IndexOutOfBoundsException("Negative offset: " + offset);

        if (materialized != null)
            return materialized.slice(offset, length);

        // if a slice of indeterminate length is requested, but the
        // length is already known, use the known length;
        final long lengthArg;
        if (this.length > 0 && length < 0)
            lengthArg = this.length - offset;
        else
            lengthArg = length;

        return new LazyReadData(lazyRead, this.offset + offset, lengthArg);
    }

    @Override
    public InputStream inputStream() throws N5IOException, IllegalStateException {
        materialize();
		return materialized.inputStream();
    }

    @Override
    public byte[] allBytes() throws N5IOException, IllegalStateException {
		materialize();
		return materialized.allBytes();
    }

    @Override
    public long length() {
        return length;
    }

	@Override
	public long requireLength() throws N5IOException {
		if (length < 0) {
			length = lazyRead.size() - offset;
		}
		return length;
	}

	@Override
	public void close() throws N5IOException {
		try {
			lazyRead.close();
		} catch (IOException e) {
			throw new N5IOException(e);
		}
	}
}
