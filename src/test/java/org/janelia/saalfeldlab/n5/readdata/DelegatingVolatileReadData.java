package org.janelia.saalfeldlab.n5.readdata;

import org.janelia.saalfeldlab.n5.N5Exception;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;

public class DelegatingVolatileReadData implements VolatileReadData {

    private final VolatileReadData delegate;

    public DelegatingVolatileReadData(VolatileReadData delegate) {
        this.delegate = delegate;
    }

    @Override
    public void close() throws N5Exception.N5IOException {
        delegate.close();
    }

    @Override
    public long length() {
        return delegate.length();
    }

    @Override
    public long requireLength() throws N5Exception.N5IOException {
        return delegate.requireLength();
    }

    @Override
    public ReadData limit(long length) throws N5Exception.N5IOException {
        return delegate.limit(length);
    }

    @Override
    public ReadData slice(long offset, long length) throws N5Exception.N5IOException {
        return delegate.slice(offset, length);
    }

    @Override
    public ReadData slice(Range range) throws N5Exception.N5IOException {
        return delegate.slice(range);
    }

    @Override
    public InputStream inputStream() throws N5Exception.N5IOException, IllegalStateException {
        return delegate.inputStream();
    }

    @Override
    public byte[] allBytes() throws N5Exception.N5IOException, IllegalStateException {
        return delegate.allBytes();
    }

    @Override
    public ByteBuffer toByteBuffer() throws N5Exception.N5IOException, IllegalStateException {
        return delegate.toByteBuffer();
    }

    @Override
    public ReadData materialize() throws N5Exception.N5IOException {
        return delegate.materialize();
    }

    @Override
    public void writeTo(OutputStream outputStream) throws N5Exception.N5IOException, IllegalStateException {
        delegate.writeTo(outputStream);
    }

    @Override
    public void prefetch(Collection<? extends Range> ranges) throws N5Exception.N5IOException {
        delegate.prefetch(ranges);
    }

    @Override
    public ReadData encode(OutputStreamOperator encoder) {
        return delegate.encode(encoder);
    }
}
