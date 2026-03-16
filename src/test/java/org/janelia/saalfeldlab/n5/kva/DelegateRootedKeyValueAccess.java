package org.janelia.saalfeldlab.n5.kva;

import java.net.URI;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
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
	public VolatileReadData createReadData(final URI normalPath) throws N5IOException {
		return kva.createReadData(normalPath);
	}

	@Override
	public boolean isDirectory(final URI normalPath) {
		return kva.isDirectory(normalPath);
	}

	@Override
	public boolean isFile(final URI normalPath) {
		return kva.isFile(normalPath);
	}

	@Override
	public boolean exists(final URI normalPath) {
		return kva.exists(normalPath);
	}

	@Override
	public long size(final URI normalPath) throws N5IOException {
		return kva.size(normalPath);
	}

	@Override
	public void write(final URI normalPath, final ReadData data) throws N5IOException {
		kva.write(normalPath, data);
	}

	@Override
	public String[] listDirectories(final URI normalPath) throws N5IOException {
		return kva.listDirectories(normalPath);
	}

	@Override
	public void createDirectories(final URI normalPath) throws N5IOException {
		kva.createDirectories(normalPath);
	}

	@Override
	public void delete(final URI normalPath) throws N5IOException {
		kva.delete(normalPath);
	}
}
