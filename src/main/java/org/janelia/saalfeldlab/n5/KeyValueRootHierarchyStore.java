package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.cache.HierarchyStore;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

/**
 * Implements {@link HierarchyStore} operations on a {@link KeyValueRoot}.
 */
public class KeyValueRootHierarchyStore implements HierarchyStore {

	private final KeyValueRoot kvr;

	public KeyValueRootHierarchyStore(final KeyValueRoot kvr) {

		this.kvr = kvr;
	}

	@Override
	public JsonElement store_readAttributesJson(
			final N5DirectoryPath group,
			final String filename,
			final Gson gson) throws N5IOException {

		final N5FilePath attributesPath = group.resolve(filename).asFile();
		try (final VolatileReadData readData = kvr.createReadData(attributesPath);) {
			// TODO: this (ReadData --> JsonElement) should go into GsonUtils?
			return GsonUtils.readAttributes(new InputStreamReader(readData.inputStream()), gson);
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final UncheckedIOException | N5IOException e) {
			throw new N5IOException("Failed to read attributes from " + attributesPath, e);
		}
	}

	@Override
	public boolean store_isDirectory(final N5DirectoryPath group) {

		return kvr.isDirectory(group);
	}

	@Override
	public String[] store_listDirectories(final N5DirectoryPath group) throws N5IOException {

		return kvr.listDirectories(group);
	}

	@Override
	public void store_writeAttributesJson(
			final N5DirectoryPath group,
			final String filename,
			final JsonElement attributes,
			final Gson gson) throws N5IOException {

		final N5FilePath attributesPath = group.resolve(filename).asFile();

		// TODO: this (JsonElement --> ReadData) should go into GsonUtils?
		final ReadData attributesReadData = ReadData.from(os -> {
			final OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8);
			GsonUtils.writeAttributes(writer, attributes, gson);
		});

		try {
			kvr.write(attributesPath, attributesReadData);
		} catch (UncheckedIOException | N5IOException e) {
			throw new N5IOException("Failed to write attributes to " + attributesPath, e);
		}
	}

	@Override
	public void store_removeDirectory(final N5DirectoryPath group) throws N5IOException {

		kvr.delete(group);
	}

	@Override
	public void store_createDirectories(N5DirectoryPath group) throws N5IOException {

		kvr.createDirectories(group);
	}
}
