package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import java.io.IOException;

interface IoPolicy {

    void write(String key, ReadData readData) throws IOException;

    VolatileReadData read(String key) throws IOException;

    void delete(String key) throws IOException;

    static IoPolicy withFallback(IoPolicy primary, IoPolicy fallback) {
        return new IoPolicy() {

            @Override
            public void write(String key, ReadData readData) throws IOException {
                try {
                    primary.write(key, readData);
                } catch (IOException e) {
                    fallback.write(key, readData);
                }
            }

            @Override
            public VolatileReadData read(String key) throws IOException {
                try {
                    return primary.read(key);
                } catch (IOException e) {
                    return fallback.read(key);
                }
            }

            @Override
            public void delete(String key) throws IOException {
                try {
                    primary.delete(key);
                } catch (IOException e) {
                    fallback.delete(key);
                }
            }
        };
    }
}
