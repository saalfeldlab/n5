/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.http;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.CachedGsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.GsonKeyValueN5Writer;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

public class HttpReaderFsWriter implements GsonKeyValueN5Writer {

	private final GsonKeyValueN5Writer writer;
	private final GsonKeyValueN5Reader reader;

	public <W extends GsonKeyValueN5Writer, R extends GsonKeyValueN5Reader> HttpReaderFsWriter(final W writer, final R reader) {
	
		this.writer = writer;
		this.reader = reader;

		if (reader instanceof CachedGsonKeyValueN5Reader && writer instanceof CachedGsonKeyValueN5Writer) {
			final CachedGsonKeyValueN5Reader cachedReader = (CachedGsonKeyValueN5Reader)reader;
			final CachedGsonKeyValueN5Writer cachedWriter = (CachedGsonKeyValueN5Writer)writer;
			if (cachedReader.cacheMeta()) {
				/* Hack necessary to test HTTP reader caching without creating the data entirely first */
				try {
					// Access the private 'cache' field in the reader (or the N5KeyValueReader as a fallback)
					Field cacheField;
					try {
						cacheField = reader.getClass().getDeclaredField("cache");
					} catch (NoSuchFieldException e) {
						cacheField = N5KeyValueReader.class.getDeclaredField("cache");
					}
					cacheField.setAccessible(true);

					// Set the value of 'cache' to the one from writer.getCache()
					cacheField.set(reader, cachedWriter.getCache());
				} catch (NoSuchFieldException | IllegalAccessException e) {
					throw new RuntimeException("Failed to set reader cache reflectively", e);
				}
			}
		}


	}

	@Override public String getAttributesKey() {

		return writer.getAttributesKey();
	}

	@Override public Version getVersion() throws N5Exception {

		return reader.getVersion();
	}

	@Override public URI getURI() {

		return reader.getURI();
	}

	@Override public <T> T getAttribute(String pathName, String key, Class<T> clazz) throws N5Exception {

		return reader.getAttribute(pathName, key, clazz);
	}

	@Override public <T> T getAttribute(String pathName, String key, Type type) throws N5Exception {

		return reader.getAttribute(pathName, key, type);
	}

	@Override public DatasetAttributes getDatasetAttributes(String pathName) throws N5Exception {

		return reader.getDatasetAttributes(pathName);
	}

	@Override public DataBlock<?> readBlock(String pathName, DatasetAttributes datasetAttributes, long... gridPosition) throws N5Exception {

		return reader.readBlock(pathName, datasetAttributes, gridPosition);
	}

	@Override public <T> T readSerializedBlock(String dataset, DatasetAttributes attributes, long... gridPosition) throws N5Exception, ClassNotFoundException {

		return reader.readSerializedBlock(dataset, attributes, gridPosition);
	}

	@Override public KeyValueAccess getKeyValueAccess() {

		return reader.getKeyValueAccess();
	}

	@Override public boolean exists(String pathName) {

		return reader.exists(pathName);
	}

	@Override public boolean datasetExists(String pathName) throws N5Exception {

		return reader.datasetExists(pathName);
	}

	@Override public String[] list(String pathName) throws N5Exception {

		return reader.list(pathName);
	}

	@Override public String[] deepList(String pathName, Predicate<String> filter) throws N5Exception {

		return reader.deepList(pathName, filter);
	}

	@Override public String[] deepList(String pathName) throws N5Exception {

		return reader.deepList(pathName);
	}

	@Override public String[] deepListDatasets(String pathName, Predicate<String> filter) throws N5Exception {

		return reader.deepListDatasets(pathName, filter);
	}

	@Override public String[] deepListDatasets(String pathName) throws N5Exception {

		return reader.deepListDatasets(pathName);
	}

	@Override public String[] deepList(String pathName, Predicate<String> filter, ExecutorService executor) throws N5Exception, InterruptedException, ExecutionException {

		return reader.deepList(pathName, filter, executor);
	}

	@Override public String[] deepList(String pathName, ExecutorService executor) throws N5Exception, InterruptedException, ExecutionException {

		return reader.deepList(pathName, executor);
	}

	@Override public String[] deepListDatasets(String pathName, Predicate<String> filter, ExecutorService executor) throws N5Exception, InterruptedException, ExecutionException {

		return reader.deepListDatasets(pathName, filter, executor);
	}

	@Override public String[] deepListDatasets(String pathName, ExecutorService executor) throws N5Exception, InterruptedException, ExecutionException {

		return reader.deepListDatasets(pathName, executor);
	}

	@Override public Gson getGson() {

		return reader.getGson();
	}

	@Override public Map<String, Class<?>> listAttributes(String pathName) throws N5Exception {

		return reader.listAttributes(pathName);
	}

	@Override public String getGroupSeparator() {

		return reader.getGroupSeparator();
	}

	@Override public String groupPath(String... nodes) {

		return reader.groupPath(nodes);
	}

	@Override public void close() {

		reader.close();
		writer.close();
	}

	@Override public <T> void setAttribute(String groupPath, String attributePath, T attribute) throws N5Exception {

		writer.setAttribute(groupPath, attributePath, attribute);
	}

	@Override public void setAttributes(String groupPath, Map<String, ?> attributes) throws N5Exception {
		writer.setAttributes(groupPath, attributes);
	}

	@Override public boolean removeAttribute(String groupPath, String attributePath) throws N5Exception {

		return writer.removeAttribute(groupPath, attributePath);
	}

	@Override public <T> T removeAttribute(String groupPath, String attributePath, Class<T> clazz) throws N5Exception {

		return writer.removeAttribute(groupPath, attributePath, clazz);
	}

	@Override public boolean removeAttributes(String groupPath, List<String> attributePaths) throws N5Exception {

		return writer.removeAttributes(groupPath, attributePaths);
	}

	@Override public void setDatasetAttributes(String datasetPath, DatasetAttributes datasetAttributes) throws N5Exception {

		writer.setDatasetAttributes(datasetPath, datasetAttributes);
	}

	@Override public void setVersion() throws N5Exception {

		writer.setVersion();
	}

	@Override public void createGroup(String groupPath) throws N5Exception {
		writer.createGroup(groupPath);
	}

	@Override public boolean remove(String groupPath) throws N5Exception {

		return writer.remove(groupPath);
	}

	@Override public boolean remove() throws N5Exception {

		return writer.remove();
	}

	@Override public DatasetAttributes createDataset(String datasetPath, DatasetAttributes datasetAttributes) throws N5Exception {

		writer.createDataset(datasetPath, datasetAttributes);
		return datasetAttributes;
	}

	@Override public <T> void writeBlock(String datasetPath, DatasetAttributes datasetAttributes, DataBlock<T> dataBlock) throws N5Exception {
		writer.writeBlock(datasetPath, datasetAttributes, dataBlock);
	}

	@Override public boolean deleteBlock(String datasetPath, long... gridPosition) throws N5Exception {

		return writer.deleteBlock(datasetPath, gridPosition);
	}

	@Override public void writeSerializedBlock(Serializable object, String datasetPath, DatasetAttributes datasetAttributes, long... gridPosition) throws N5Exception {

		writer.writeSerializedBlock(object, datasetPath, datasetAttributes, gridPosition);
	}

	@Override public void setVersion(String path) {

		writer.setVersion(path);
	}

	@Override public void writeAttributes(String normalGroupPath, JsonElement attributes) throws N5Exception {

		writer.writeAttributes(normalGroupPath, attributes);
	}

	@Override public void setAttributes(String path, JsonElement attributes) throws N5Exception {

		writer.setAttributes(path, attributes);
	}

	@Override public void writeAttributes(String normalGroupPath, Map<String, ?> attributes) throws N5Exception {

		writer.writeAttributes(normalGroupPath, attributes);
	}

	@Override public <T> void writeBlocks(String datasetPath, DatasetAttributes datasetAttributes, DataBlock<T>... dataBlocks) throws N5Exception {

		writer.writeBlocks(datasetPath, datasetAttributes, dataBlocks);
	}
}
