package org.janelia.saalfeldlab.n5.http;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.cache.HierarchyStore;

public class ReadWriteMetaStore implements HierarchyStore {

	private final HierarchyStore readStore;
	private final HierarchyStore writeStore;

	public ReadWriteMetaStore(final HierarchyStore readStore, final HierarchyStore writeStore) {
		this.readStore = readStore;
		this.writeStore = writeStore;
	}

	@Override
	public JsonElement store_readAttributesJson(final N5DirectoryPath group, final String filename, final Gson gson) throws N5IOException {
		return readStore.store_readAttributesJson(group, filename, gson);
	}

	@Override
	public boolean store_isDirectory(final N5DirectoryPath group) {
		return readStore.store_isDirectory(group);
	}

	@Override
	public String[] store_listDirectories(final N5DirectoryPath group) throws N5IOException {
		return readStore.store_listDirectories(group);
	}

	@Override
	public void store_writeAttributesJson(final N5DirectoryPath group, final String filename, final JsonElement attributes, final Gson gson) throws N5IOException {
		writeStore.store_writeAttributesJson(group, filename, attributes, gson);
	}

	@Override
	public void store_removeAttributesJson(final N5DirectoryPath group, final String filename) throws N5IOException {
		writeStore.store_removeAttributesJson(group, filename);
	}

	@Override
	public void store_createDirectories(final N5DirectoryPath group) throws N5IOException {
		writeStore.store_createDirectories(group);
	}

	@Override
	public void store_removeDirectory(final N5DirectoryPath group) throws N5IOException {
		writeStore.store_removeDirectory(group);
	}
}
