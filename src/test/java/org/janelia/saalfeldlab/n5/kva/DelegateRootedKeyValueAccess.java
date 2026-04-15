package org.janelia.saalfeldlab.n5.kva;

import java.net.URI;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.RootedKeyValueAccess;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

public class DelegateRootedKeyValueAccess implements RootedKeyValueAccess {

	protected final RootedKeyValueAccess kva;

	public DelegateRootedKeyValueAccess(RootedKeyValueAccess kva) {
		this.kva = kva;
	}

	@Deprecated
	@Override
	public KeyValueAccess getKVA() {
		return kva.getKVA();
	}

	@Override
	public URI root() {
		return kva.root();
	}

	@Override
	public VolatileReadData createReadData(final N5FilePath normalPath) throws N5IOException {
		return kva.createReadData(normalPath);
	}

	@Override
	public boolean isDirectory(final N5Path normalPath) {
		return kva.isDirectory(normalPath);
	}

	@Override
	public boolean isFile(final N5Path normalPath) {
		return kva.isFile(normalPath);
	}

	@Override
	public boolean exists(final N5Path normalPath) {
		return kva.exists(normalPath);
	}

	@Override
	public long size(final N5FilePath normalPath) throws N5IOException {
		return kva.size(normalPath);
	}

	@Override
	public void write(final N5FilePath normalPath, final ReadData data) throws N5IOException {
		kva.write(normalPath, data);
	}

	@Override
	public String[] listDirectories(final N5DirectoryPath normalPath) throws N5IOException {
		return kva.listDirectories(normalPath);
	}

	@Override
	public void createDirectories(final N5DirectoryPath normalPath) throws N5IOException {
		kva.createDirectories(normalPath);
	}

	@Override
	public void delete(final N5Path normalPath) throws N5IOException {
		kva.delete(normalPath);
	}
}
