package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;

public class TrackingHierarchyStore implements HierarchyStore {

	private final HierarchyStore delegate;
	private final HierarchyStoreCounters counters = new HierarchyStoreCounters();

	public TrackingHierarchyStore(final HierarchyStore delegate) {
		this.delegate = delegate;
	}

	public HierarchyStoreCounters counters() {
		return counters;
	}

	@Override
	public JsonElement readAttributesJson(final N5DirectoryPath parent, final String filename, final Gson gson) throws N5Exception.N5IOException {
		counters.incReadAttr();
		return delegate.readAttributesJson(parent, filename, gson);
	}

	@Override
	public void writeAttributesJson(final N5DirectoryPath parent, final String filename, final JsonElement attributes, final Gson gson) throws
			N5Exception.N5IOException {
		counters.incWriteAttr();
		delegate.writeAttributesJson(parent, filename, attributes, gson);
	}

	@Override
	public boolean isDirectory(final N5DirectoryPath path) {
		counters.incIsDir();
		return delegate.isDirectory(path);
	}

	@Override
	public void removeDirectory(final N5DirectoryPath path) throws N5Exception.N5IOException {
		counters.incRmDir();
		delegate.removeDirectory(path);
	}

	@Override
	public void createDirectories(final N5DirectoryPath path) throws N5Exception.N5IOException {
		counters.incMkDir();
		delegate.createDirectories(path);
	}

	@Override
	public String[] listDirectories(final N5DirectoryPath path) throws N5Exception.N5IOException {
		counters.incList();
		return delegate.listDirectories(path);
	}
}
