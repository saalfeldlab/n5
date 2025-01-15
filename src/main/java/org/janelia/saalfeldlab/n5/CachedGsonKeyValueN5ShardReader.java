/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;
import org.janelia.saalfeldlab.n5.shard.Shard;
import org.janelia.saalfeldlab.n5.shard.ShardParameters;
import org.janelia.saalfeldlab.n5.shard.VirtualShard;

public interface CachedGsonKeyValueN5ShardReader
		extends CachedGsonKeyValueN5Reader, N5JsonCacheableContainer, N5ShardReader {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	default <A extends DatasetAttributes & ShardParameters> Shard<?> readShard(final String pathName,
			final A datasetAttributes, long... shardGridPosition) {

		final String path = absoluteDataBlockPath(N5URI.normalizeGroupPath(pathName), shardGridPosition);
		return new VirtualShard(datasetAttributes, shardGridPosition, getKeyValueAccess(), path);
	}

	@Override
	default DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		if (datasetAttributes instanceof ShardedDatasetAttributes) {
			final ShardedDatasetAttributes shardedAttrs = (ShardedDatasetAttributes) datasetAttributes;
			final long[] shardPosition = shardedAttrs.getShardPositionForBlock(gridPosition);
			final Shard<?> shard = readShard(pathName, shardedAttrs, shardPosition);
			return shard.getBlock(gridPosition);
		}

		return CachedGsonKeyValueN5Reader.super.readBlock(pathName, datasetAttributes, gridPosition);
	}
	
	@Override
	default List<DataBlock<?>> readBlocks(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final List<long[]> blockPositions) throws N5Exception {

		// TODO which interface should have this implementation?
		if (datasetAttributes instanceof ShardParameters) {

			/* Group by shard index */
			final HashMap<Integer, Shard<?>> shardBlockMap = new HashMap<>();
			final HashMap<Integer, List<long[]>> shardPositionMap = new HashMap<>();
			final ShardParameters shardAttributes = (ShardParameters) datasetAttributes;

			for (long[] blockPosition : blockPositions) {
				final long[] shardPosition = shardAttributes.getShardPositionForBlock(blockPosition);
				final int shardHash = Arrays.hashCode(shardPosition);
				if (!shardBlockMap.containsKey(shardHash)) {
					final Shard<?> shard = readShard(pathName, (DatasetAttributes & ShardParameters) shardAttributes,
							shardPosition);
					shardBlockMap.put(shardHash, shard);

					final ArrayList<long[]> positionList = new ArrayList<>();
					positionList.add(blockPosition);
					shardPositionMap.put(shardHash, positionList);
				} else
					shardPositionMap.get(shardBlockMap.get(shardHash)).add(blockPosition);
			}

			final ArrayList<DataBlock<?>> blocks = new ArrayList<>();
			for (Shard<?> shard : shardBlockMap.values()) {
				/* Add existing blocks before overwriting shard */
				final int shardHash = Arrays.hashCode(shard.getGridPosition());
				for (final long[] blkPosition : shardPositionMap.get(shardHash)) {
					blocks.add(shard.getBlock(blkPosition));
				}
			}
			return blocks;
		} else
			return CachedGsonKeyValueN5Reader.super.readBlocks(pathName, datasetAttributes, blockPositions);
	}

}
