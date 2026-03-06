package org.janelia.saalfeldlab.n5;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * Supports default implementation of the deprecated {@link KeyValueAccess#lockForReading(String)} and {@link KeyValueAccess#lockForWriting(String)} methods.
 */
class BufferedKvaLockedChannel implements LockedChannel {

    private final KeyValueAccess kva;
    private final String key;
    private ByteArrayOutputStream baos = null;

    BufferedKvaLockedChannel(final KeyValueAccess kva, final String key) {
        this.kva = kva;
        this.key = key;
    }

    @Override
    public Reader newReader() throws N5IOException {

        return new InputStreamReader(newInputStream());
    }

    @Override
    public InputStream newInputStream() throws N5IOException {
        return kva.createReadData(key).inputStream();
    }

    @Override
    public Writer newWriter() throws N5IOException {

        return new BufferedWriter(new OutputStreamWriter(newOutputStream()));
    }

    @Override
    public OutputStream newOutputStream() throws N5IOException {
        if (baos == null)
            baos = new ByteArrayOutputStream();
        return baos;
    }

    @Override
    public void close() throws IOException {
        if (baos != null && baos.size() > 0)
            kva.write(key, ReadData.from(baos.toByteArray()));
    }
}
