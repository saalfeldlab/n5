package org.janelia.saalfeldlab.n5;

import org.apache.commons.io.input.ProxyInputStream;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import java.io.*;

class BufferedKvaLockedChannel implements LockedChannel {

    private final KeyValueAccess kva;
    private final String key;
    private ByteArrayOutputStream baos = null;

    BufferedKvaLockedChannel(final KeyValueAccess kva, final String key) {
        this.kva = kva;
        this.key = key;
    }

    @Override
    public Reader newReader() throws N5Exception.N5IOException {

        return new InputStreamReader(newInputStream());
    }

    @Override
    public InputStream newInputStream() throws N5Exception.N5IOException {

        VolatileReadData volatileReadData = kva.createReadData(key);
        return new ProxyInputStream(volatileReadData.inputStream()) {
            @Override
            public void close() throws IOException {
                volatileReadData.close();
                super.close();
            }
        };
    }

    @Override
    public Writer newWriter() throws N5Exception.N5IOException {

        return new BufferedWriter(new OutputStreamWriter(newOutputStream()));
    }

    @Override
    public OutputStream newOutputStream() throws N5Exception.N5IOException {
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
