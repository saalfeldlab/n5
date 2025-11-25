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
import java.util.function.Supplier;

import org.apache.commons.io.input.ClosedInputStream;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

/**
 * Lazy InputStream implementation that only materializes the delegate on the first read.
 * This class provides a reusable way to create InputStreams that defer data loading
 * until the first read operation.
 */
class LazyInputStream extends InputStream {
    private final Supplier<InputStream> inputStreamSupplier;
    private InputStream delegateStream = null;
    
    /**
     * Creates a LazyInputStream backed by the given Supplier<InputStream>.
     * The InputStream will only be materialized when the stream is first read.
     * 
     * @param inputStreamSupplier the input stream to materialize lazily (on first read)
     */
    LazyInputStream(final Supplier<InputStream> inputStreamSupplier) {
        this.inputStreamSupplier = inputStreamSupplier;
    }

    private synchronized InputStream getDelegate() {
        if (delegateStream == null) {
            delegateStream = inputStreamSupplier.get();
        }
        return delegateStream;
    }
    
    @Override
    public int read() throws IOException {
        return getDelegate().read();
    }
    
    @Override
    public int read(byte[] b) throws IOException {
        return getDelegate().read(b);
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return getDelegate().read(b, off, len);
    }
    
    @Override
    public long skip(long n) throws IOException {
        return getDelegate().skip(n);
    }
    
    @Override
    public int available() throws IOException {
        if (delegateStream == null) {
            // 0 is a bit confusing since it typically indicates end-of-stream, but
            // in this case it's 0 at the start of the stream
            return 0;
        }
        return delegateStream.available();
    }
    
    @Override
    public synchronized void close() throws IOException {
        if (delegateStream == null) {
            this.delegateStream = ClosedInputStream.INSTANCE;
        }
        delegateStream.close();
    }
    
    @Override
    public void mark(int readlimit) {
        if (delegateStream != null) {
            delegateStream.mark(readlimit);
        }
    }
    
    @Override
    public void reset() throws IOException {
        if (delegateStream == null) {
            throw new IOException("Stream not yet initialized - cannot reset");
        }
        delegateStream.reset();
    }
    
    @Override
    public boolean markSupported() {
        if (delegateStream == null) {
            return false;
        }
        return delegateStream.markSupported();
    }
}