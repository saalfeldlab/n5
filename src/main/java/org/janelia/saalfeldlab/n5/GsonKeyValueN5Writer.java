package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.janelia.saalfeldlab.n5.shard.PositionValueAccess;

/**
 * Default implementation of {@link N5Writer} with JSON attributes parsed with
 * {@link Gson}.
 */
public interface GsonKeyValueN5Writer extends GsonN5Writer, GsonKeyValueN5Reader {

	@Override
	default void createGroup(final String path) throws N5Exception {

		getContainerDialect().createGroup(N5DirectoryPath.of(path));
	}

	@Override
	default DatasetAttributes createDataset(final String datasetPath, final DatasetAttributes datasetAttributes) throws N5Exception {

		final DatasetAttributes attributes = getConvertedDatasetAttributes(datasetAttributes);
		getContainerDialect().createDataset(N5DirectoryPath.of(datasetPath), attributes);
		return attributes;
	}

	@Override
	default void setAttributes(final String path, final Map<String, ?> attributes) throws N5Exception {

		getContainerDialect().setAttributes(N5DirectoryPath.of(path), attributes);
	}

	@Override
	default boolean removeAttribute(final String path, final String attributePath) throws N5Exception {

		return getContainerDialect().removeAttribute(N5DirectoryPath.of(path), attributePath);
	}

	@Override
	default <T> T removeAttribute(final String path, final String attributePath, final Class<T> clazz) throws N5Exception {

		return getContainerDialect().removeAttribute(N5DirectoryPath.of(path), attributePath, clazz);
	}

	@Override
	default boolean remove(final String path) throws N5Exception {

		return getContainerDialect().remove(N5DirectoryPath.of(path));
	}

	@Override
	default <T> void writeRegion(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> chunkSupplier,
			final boolean writeFully) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(datasetPath), convertedDatasetAttributes);
			convertedDatasetAttributes.<T>getDatasetAccess().writeRegion(pva, min, size, chunkSupplier, writeFully);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write blocks into dataset " + datasetPath, e);
		}
	}

	@Override
	default <T> void writeRegion(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final long[] min,
			final long[] size,
			final DataBlockSupplier<T> chunkSupplier,
			final boolean writeFully,
			final ExecutorService exec) throws N5Exception, InterruptedException, ExecutionException {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(datasetPath), convertedDatasetAttributes);
			convertedDatasetAttributes.<T>getDatasetAccess().writeRegion(pva, min, size, chunkSupplier, writeFully, exec);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write blocks into dataset " + datasetPath, e);
		}
	}

	@Override
	default <T> void writeChunks(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T>... chunks) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(datasetPath), convertedDatasetAttributes);
			convertedDatasetAttributes.<T>getDatasetAccess().writeChunks(pva, Arrays.asList(chunks));
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write chunks into dataset " + datasetPath, e);
		}
	}

	@Override
	default <T> void writeChunk(
			final String path,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> chunk) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		try {
			final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(path), convertedDatasetAttributes);
			convertedDatasetAttributes.<T> getDatasetAccess().writeChunk(pva, chunk);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write chunk " + Arrays.toString(chunk.getGridPosition()) + " into dataset " + path,
					e);
		}
	}

	@Override
	default <T> void writeBlock(
			final String path,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T> dataBlock) throws N5Exception {

		final DatasetAttributes convertedDatasetAttributes = getConvertedDatasetAttributes(datasetAttributes);
		final int shardLevel = convertedDatasetAttributes.getNestedBlockGrid().numLevels() - 1;
		try {
			final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(path), convertedDatasetAttributes);
			convertedDatasetAttributes.<T> getDatasetAccess().writeBlock(pva, dataBlock, shardLevel);
		} catch (final UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write block " + Arrays.toString(dataBlock.getGridPosition()) + " into dataset " + path,
					e);
		}
	}

	@Override
	default boolean deleteBlock(
			final String path,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(path), datasetAttributes);
		return pva.remove(gridPosition);
	}

	@Override
	default boolean deleteChunk(
			final String path,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(path), datasetAttributes);
		return datasetAttributes.getDatasetAccess().deleteChunk(pva, gridPosition);
	}

	@Override
	default boolean deleteChunks(
			final String path,
			final DatasetAttributes datasetAttributes,
			final List<long[]> gridPositions) throws N5Exception {

		final PositionValueAccess pva = PositionValueAccess.fromKeyValueRoot(getKeyValueRoot(), N5DirectoryPath.of(path), datasetAttributes);
		return datasetAttributes.getDatasetAccess().deleteChunks(pva, gridPositions);
	}
}
