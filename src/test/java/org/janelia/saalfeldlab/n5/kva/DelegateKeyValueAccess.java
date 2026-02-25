package org.janelia.saalfeldlab.n5.kva;

import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

import java.net.URI;
import java.net.URISyntaxException;

public class DelegateKeyValueAccess implements KeyValueAccess {

    protected final KeyValueAccess kva;

    public DelegateKeyValueAccess(KeyValueAccess kva) { this.kva = kva; }

    @Override
    public String[] components(String path) {
        return kva.components(path);
    }

    @Override
    public String compose(URI uri, String... components) {
        return kva.compose(uri, components);
    }

    @Override
    public String compose(String... components) {
        return kva.compose(components);
    }

    @Override
    public String parent(String path) {
        return kva.parent(path);
    }

    @Override
    public String relativize(String path, String base) {
        return kva.relativize(path, base);
    }

    @Override
    public String normalize(String path) {
        return kva.normalize(path);
    }

    @Override
    public URI uri(String uriString) throws URISyntaxException {
        return kva.uri(uriString);
    }

    @Override
    public boolean exists(String normalPath) {
        return kva.exists(normalPath);
    }

    @Override
    public long size(String normalPath) throws N5Exception.N5NoSuchKeyException {
        return kva.size(normalPath);
    }

    @Override
    public boolean isDirectory(String normalPath) {
        return kva.isDirectory(normalPath);
    }

    @Override
    public boolean isFile(String normalPath) {
        return kva.isFile(normalPath);
    }

    @Override
    public VolatileReadData createReadData(String normalPath) throws N5Exception.N5IOException {
        return kva.createReadData(normalPath);
    }

    @Override
    public void write(String normalPath, ReadData data) throws N5Exception.N5IOException {
        kva.write( normalPath, data);
    }

    @Override
    public String[] listDirectories(String normalPath) throws N5Exception.N5IOException {
        return kva.listDirectories(normalPath);
    }

    @Override
    public String[] list(String normalPath) throws N5Exception.N5IOException {
        return kva.list(normalPath);
    }

    @Override
    public void createDirectories(String normalPath) throws N5Exception.N5IOException {
        kva.createDirectories(normalPath);

    }

    @Override
    public void delete(String normalPath) throws N5Exception.N5IOException {
        kva.delete(normalPath);
    }
}
