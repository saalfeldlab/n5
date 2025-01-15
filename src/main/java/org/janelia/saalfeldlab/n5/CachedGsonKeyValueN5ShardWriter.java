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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.shard.InMemoryShard;
import org.janelia.saalfeldlab.n5.shard.Shard;
import org.janelia.saalfeldlab.n5.shard.ShardParameters;

public interface CachedGsonKeyValueN5ShardWriter extends CachedGsonKeyValueN5Writer, N5ShardWriter, CachedGsonKeyValueN5ShardReader {

	@Override
	default <T,A extends DatasetAttributes & ShardParameters> void writeShard(
			final String path,
			final A datasetAttributes,
			final Shard<T> shard) throws N5Exception {

		final String shardPath = absoluteDataBlockPath(N5URI.normalizeGroupPath(path), shard.getGridPosition());
		try (final LockedChannel lock = getKeyValueAccess().lockForWriting(shardPath)) {
			try (final OutputStream out = lock.newOutputStream()) {
				InMemoryShard.fromShard(shard).write(out);
			}
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException(
					"Failed to write shard " + Arrays.toString(shard.getGridPosition()) + " into dataset " + path, e);
		}
	}

	@Override default <T> void writeBlocks(
			final String datasetPath,
			final DatasetAttributes datasetAttributes,
			final DataBlock<T>... dataBlocks) throws N5Exception {

		if (datasetAttributes instanceof ShardParameters) {
			/* Group by shard index */
			final HashMap<Integer, InMemoryShard<T>> shardBlockMap = new HashMap<>();
			final ShardParameters shardAttributes = (ShardParameters)datasetAttributes;

			for (DataBlock<T> dataBlock : dataBlocks) {
				final long[] shardPosition = shardAttributes.getShardPositionForBlock(dataBlock.getGridPosition());
				final int shardHash = Arrays.hashCode(shardPosition);
				if (!shardBlockMap.containsKey(shardHash))
					shardBlockMap.put(shardHash, new InMemoryShard<>((DatasetAttributes & ShardParameters)shardAttributes, shardPosition));

				final InMemoryShard<T> shard = shardBlockMap.get(shardHash);
				shard.addBlock(dataBlock);
			}

			for (InMemoryShard<T> shard : shardBlockMap.values()) {
				/* Add existing blocks before overwriting shard */
				@SuppressWarnings("unchecked")
				final Shard<T> currentShard = (Shard<T>)readShard(datasetPath, (DatasetAttributes & ShardParameters)shardAttributes, shard.getGridPosition());
				for (DataBlock<T> currentBlock : currentShard.getBlocks()) {
					if (shard.getBlock(currentBlock.getGridPosition()) == null)
						shard.addBlock(currentBlock);
				}

				writeShard(datasetPath, (DatasetAttributes & ShardParameters)shardAttributes, shard);
			}

		} else {
			CachedGsonKeyValueN5Writer.super.writeBlocks(datasetPath, datasetAttributes, dataBlocks);
		}
	}
}
