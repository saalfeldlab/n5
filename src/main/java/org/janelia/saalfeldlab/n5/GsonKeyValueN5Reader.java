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
package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shard.Shard;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.shard.VirtualShard;
import org.janelia.saalfeldlab.n5.util.Position;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.Codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 */
public interface GsonKeyValueN5Reader extends GsonN5Reader {

	KeyValueAccess getKeyValueAccess();

	default boolean groupExists(final String normalPath) {

		return getKeyValueAccess().isDirectory(absoluteGroupPath(normalPath));
	}

	@Override
	default boolean exists(final String pathName) {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		return groupExists(normalPath) || datasetExists(normalPath);
	}

	@Override
	default boolean datasetExists(final String pathName) throws N5Exception {

		// for n5, every dataset must be a group
		return getDatasetAttributes(pathName) != null;
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attribute
	 * @throws N5Exception if the attributes cannot be read
	 */
	@Override
	default JsonElement getAttributes(final String pathName) throws N5Exception {

		final String groupPath = N5URI.normalizeGroupPath(pathName);
		final String attributesPath = absoluteAttributesPath(groupPath);

		try (final LockedChannel lockedChannel = getKeyValueAccess().lockForReading(attributesPath)) {
			return GsonUtils.readAttributes(lockedChannel.newReader(), getGson());
		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final IOException | UncheckedIOException | N5IOException e) {
			throw new N5IOException("Failed to read attributes from dataset " + pathName, e);
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	default <T> Shard<T> readShard(
			final String keyPath,
			final DatasetAttributes datasetAttributes,
			long... shardGridPosition) {

		final String path = absoluteDataBlockPath(N5URI.normalizeGroupPath(keyPath), shardGridPosition);
		try {
			final ReadData readData  = getKeyValueAccess().createReadData(path).materialize();
			return new VirtualShard( datasetAttributes, shardGridPosition, readData);
		} catch (N5Exception.N5NoSuchKeyException e) {
			return null;
		}
	}

	@Override
	default <T> DataBlock<T> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final long[] keyPos = datasetAttributes.getArrayCodec().getPositionForBlock(datasetAttributes, gridPosition);
		final String path = absoluteDataBlockPath(N5URI.normalizeGroupPath(pathName), keyPos);

		try {
			final ReadData decodeData = getKeyValueAccess().createReadData(path);
			return datasetAttributes.getArrayCodec().decode(decodeData, gridPosition);
		} catch (N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (IOException | UncheckedIOException | N5IOException e) {
			throw new N5IOException(
					"Failed to read block " + Arrays.toString(gridPosition) + " from dataset " + path,
					e);
		}
	}


	@Override
	default <T> List<DataBlock<T>> readBlocks(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final List<long[]> blockPositions) throws N5Exception {

		// TODO which interface should have this implementation?
		if (datasetAttributes.isSharded()) {

			/* Group by shard position */
			final Map<Position, List<long[]>> shardBlockMap = datasetAttributes.groupBlockPositions(blockPositions);
			final ArrayList<DataBlock<T>> blocks = new ArrayList<>();
			for( Map.Entry<Position, List<long[]>> e : shardBlockMap.entrySet()) {

				Shard<T> currentShard = readShard(pathName, datasetAttributes, e.getKey().get());
				if (currentShard == null)
					continue;

				for (final long[] blkPosition : e.getValue()) {
					blocks.add(currentShard.getBlock(blkPosition));
				}
			}
			return blocks;
		}
		return GsonN5Reader.super.readBlocks(pathName, datasetAttributes, blockPositions);
	}

	@Override
	default String[] list(final String pathName) throws N5Exception {

		return getKeyValueAccess().listDirectories(absoluteGroupPath(pathName));
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid
	 * position.
	 * <br>
	 * If the gridPosition passed in refers to shard position
	 * in a sharded dataset, this will return the path to the shard key
	 * <p>
	 * The returned path is
	 *
	 * <pre>
	 * $basePath/datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 * <p>
	 * This is the file into which the data block will be stored.
	 *
	 * @param normalPath
	 *            normalized dataset path
	 * @param gridPosition to the target data block
	 * @return the absolute path to the data block ad gridPosition
	 */
	default String absoluteDataBlockPath(
			final String normalPath,
			final long... gridPosition) {

		final String[] components = new String[gridPosition.length + 1];
		components[0] = normalPath;
		int i = 0;
		for (final long p : gridPosition)
			components[++i] = Long.toString(p);

		return getKeyValueAccess().compose(getURI(), components);
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the group or
	 * dataset.
	 *
	 * @param normalGroupPath
	 *            normalized group path without leading slash
	 * @return the absolute path to the group
	 */
	default String absoluteGroupPath(final String normalGroupPath) {

		return getKeyValueAccess().compose(getURI(), normalGroupPath);
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the attributes
	 * file of a group or dataset.
	 *
	 * @param normalPath
	 *            normalized group path without leading slash
	 * @return the absolute path to the attributes
	 */
	default String absoluteAttributesPath(final String normalPath) {

		return getKeyValueAccess().compose(getURI(), normalPath, getAttributesKey());
	}
}
