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
import org.janelia.saalfeldlab.n5.cache.DelegateStore;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.VolatileReadData;

public class KeyValueAccessMetaStore implements DelegateStore {

	private final KeyValueRoot kva;

	public KeyValueAccessMetaStore(final KeyValueRoot kva) {

		this.kva = kva;
	}

	@Override
	public JsonElement store_readAttributesJson(
			final N5DirectoryPath group,
			final String filename,
			final Gson gson) throws N5IOException {

		final N5FilePath attributesPath = group.resolve(filename).asFile();
		try (final VolatileReadData readData = kva.createReadData(attributesPath);) {
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

		return kva.isDirectory(group);
	}

	@Override
	public String[] store_listDirectories(final N5DirectoryPath group) throws N5IOException {

		return kva.listDirectories(group);
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
			kva.write(attributesPath, attributesReadData);
		} catch (UncheckedIOException | N5IOException e) {
			throw new N5IOException("Failed to write attributes to " + attributesPath, e);
		}
	}

	@Override
	public void store_removeAttributesJson(
			final N5DirectoryPath group,
			final String filename) throws N5IOException {

		final N5FilePath file = group.resolve(filename).asFile();
		kva.delete(file);
	}

	@Override
	public void store_removeDirectory(final N5DirectoryPath group) throws N5IOException {

		kva.delete(group);
	}

	@Override
	public void store_createDirectories(N5DirectoryPath group) throws N5IOException {

		kva.createDirectories(group);
	}
}
