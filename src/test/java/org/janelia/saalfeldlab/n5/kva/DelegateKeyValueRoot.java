package org.janelia.saalfeldlab.n5.kva;

import java.net.URI;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.KeyValueRoot;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

public class DelegateKeyValueRoot implements KeyValueRoot {

	protected final KeyValueRoot kvr;

	public DelegateKeyValueRoot(KeyValueRoot kvr) {
		this.kvr = kvr;
	}

	@Deprecated
	@Override
	public KeyValueAccess getKVA() {
		return kvr.getKVA();
	}

	@Override
	public URI uri() {
		return kvr.uri();
	}

	@Override
	public VolatileReadData createReadData(final N5FilePath normalPath) throws N5IOException {
		return kvr.createReadData(normalPath);
	}

	@Override
	public boolean isDirectory(final N5Path normalPath) {
		return kvr.isDirectory(normalPath);
	}

	@Override
	public boolean isFile(final N5Path normalPath) {
		return kvr.isFile(normalPath);
	}

	@Override
	public boolean exists(final N5Path normalPath) {
		return kvr.exists(normalPath);
	}

	@Override
	public long size(final N5FilePath normalPath) throws N5IOException {
		return kvr.size(normalPath);
	}

	@Override
	public void write(final N5FilePath normalPath, final ReadData data) throws N5IOException {
		kvr.write(normalPath, data);
	}

	@Override
	public String[] listDirectories(final N5DirectoryPath normalPath) throws N5IOException {
		return kvr.listDirectories(normalPath);
	}

	@Override
	public void createDirectories(final N5DirectoryPath normalPath) throws N5IOException {
		kvr.createDirectories(normalPath);
	}

	@Override
	public void delete(final N5Path normalPath) throws N5IOException {
		kvr.delete(normalPath);
	}
}
