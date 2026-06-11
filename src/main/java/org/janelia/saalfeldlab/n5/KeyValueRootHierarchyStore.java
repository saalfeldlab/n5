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
	public JsonElement readAttributesJson(
			final N5DirectoryPath parent,
			final String filename,
			final Gson gson) throws N5IOException {

		final N5FilePath attributesPath = parent.resolve(filename).asFile();
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
	public boolean isDirectory(final N5DirectoryPath path) {

		return kvr.isDirectory(path);
	}

	@Override
	public String[] listDirectories(final N5DirectoryPath path) throws N5IOException {

		return kvr.listDirectories(path);
	}

	@Override
	public void writeAttributesJson(
			final N5DirectoryPath parent,
			final String filename,
			final JsonElement attributes,
			final Gson gson) throws N5IOException {

		final N5FilePath attributesPath = parent.resolve(filename).asFile();

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
	public void removeDirectory(final N5DirectoryPath path) throws N5IOException {

		kvr.delete(path);
	}

	@Override
	public void createDirectories(N5DirectoryPath path) throws N5IOException {

		kvr.createDirectories(path);
	}
}
