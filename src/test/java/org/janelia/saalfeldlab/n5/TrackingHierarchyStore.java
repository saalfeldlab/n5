package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.cache.HierarchyStore;

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
	public JsonElement store_readAttributesJson(final N5Path.N5DirectoryPath group, final String filename, final Gson gson) throws N5Exception.N5IOException {
		counters.incReadAttr();
		return delegate.store_readAttributesJson(group, filename, gson);
	}

	@Override
	public void store_writeAttributesJson(final N5Path.N5DirectoryPath group, final String filename, final JsonElement attributes, final Gson gson) throws
			N5Exception.N5IOException {
		counters.incWriteAttr();
		delegate.store_writeAttributesJson(group, filename, attributes, gson);
	}

	@Override
	public void store_removeAttributesJson(final N5Path.N5DirectoryPath group, final String filename) throws N5Exception.N5IOException {
		counters.incRemoveAttr();
		delegate.store_removeAttributesJson(group, filename);
	}

	@Override
	public boolean store_isDirectory(final N5Path.N5DirectoryPath group) {
		counters.incIsDir();
		return delegate.store_isDirectory(group);
	}

	@Override
	public void store_removeDirectory(final N5Path.N5DirectoryPath group) throws N5Exception.N5IOException {
		counters.incRmDir();
		delegate.store_removeDirectory(group);
	}

	@Override
	public void store_createDirectories(final N5Path.N5DirectoryPath group) throws N5Exception.N5IOException {
		counters.incMkDir();
		delegate.store_createDirectories(group);
	}

	@Override
	public String[] store_listDirectories(final N5Path.N5DirectoryPath group) throws N5Exception.N5IOException {
		counters.incList();
		return delegate.store_listDirectories(group);
	}
}
