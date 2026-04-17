package org.janelia.saalfeldlab.n5.http;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.cache.HierarchyStore;

public class ReadWriteHierarchyStore implements HierarchyStore {

	private final HierarchyStore readStore;
	private final HierarchyStore writeStore;

	public ReadWriteHierarchyStore(final HierarchyStore readStore, final HierarchyStore writeStore) {
		this.readStore = readStore;
		this.writeStore = writeStore;
	}

	@Override
	public JsonElement readAttributesJson(final N5DirectoryPath parent, final String filename, final Gson gson) throws N5IOException {
		return readStore.readAttributesJson(parent, filename, gson);
	}

	@Override
	public boolean isDirectory(final N5DirectoryPath path) {
		return readStore.isDirectory(path);
	}

	@Override
	public String[] listDirectories(final N5DirectoryPath path) throws N5IOException {
		return readStore.listDirectories(path);
	}

	@Override
	public void writeAttributesJson(final N5DirectoryPath parent, final String filename, final JsonElement attributes, final Gson gson) throws N5IOException {
		writeStore.writeAttributesJson(parent, filename, attributes, gson);
	}

	@Override
	public void createDirectories(final N5DirectoryPath path) throws N5IOException {
		writeStore.createDirectories(path);
	}

	@Override
	public void removeDirectory(final N5DirectoryPath path) throws N5IOException {
		writeStore.removeDirectory(path);
	}
}
