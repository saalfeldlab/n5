package org.janelia.saalfeldlab.n5.http;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

class HttpReaderFsWriter implements N5Writer {

	private final N5Writer writer;
	private final N5Reader reader;

	HttpReaderFsWriter(final N5Writer writer, final N5Reader reader) {

		this.writer = writer;
		this.reader = reader;
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

	@Override public void createDataset(String datasetPath, DatasetAttributes datasetAttributes) throws N5Exception {

		writer.createDataset(datasetPath, datasetAttributes);
	}

	@Override public void createDataset(String datasetPath, long[] dimensions, int[] blockSize, DataType dataType, Compression compression) throws N5Exception {

		writer.createDataset(datasetPath, dimensions, blockSize, dataType, compression);
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
}
