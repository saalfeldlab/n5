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
package org.janelia.saalfeldlab.n5.shard;

import static org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation.START;

import java.util.ArrayList;
import java.util.List;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.Segment;
import org.janelia.saalfeldlab.n5.readdata.Range;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.NDArray;

public class RawShardCodec implements BlockCodec<RawShard> {

	/**
	 * Number of elements (DataBlocks, nested shards) in each dimension per shard.
	 */
	private final int[] size;
	private final IndexLocation indexLocation;
	private final BlockCodec<long[]> indexCodec;
	private final long indexBlockSizeInBytes;

	RawShardCodec(final int[] size, final IndexLocation indexLocation, final BlockCodec<long[]> indexCodec) {

		this.size = size;
		this.indexLocation = indexLocation;
		this.indexCodec = indexCodec;
		indexBlockSizeInBytes = indexCodec.encodedSize(ShardIndex.blockSizeFromIndexSize(size));
	}

	@Override
	public ReadData encode(final DataBlock<RawShard> shard) throws N5Exception.N5IOException {

		// concatenate slices for all non-null segments in shard.getData().index()
		final NDArray<Segment> index = shard.getData().index();
		final List<SegmentedReadData> readDatas = new ArrayList<>();
		// TODO: Any clever ReadData grouping, slice merging, etc. should go here
		//       This basic implementation just slices ReadData for all non-null
		//       elements and concatenates in flat index order.
		for (Segment segment : index.data) {
			if (segment != null) {
				readDatas.add(segment.source().slice(segment));
			}
		}
		final SegmentedReadData data = SegmentedReadData.concatenate(readDatas);

		final ReadData.Generator writer;
		if (indexLocation == START) {
			data.materialize();
			final NDArray<Range> locations = ShardIndex.locations(index, data);
			final DataBlock<long[]> indexDataBlock = ShardIndex.toDataBlock(locations, indexBlockSizeInBytes);
			final ReadData indexReadData = indexCodec.encode(indexDataBlock);
			writer = out -> {
				indexReadData.writeTo(out);
				data.writeTo(out);
			};
		} else { // indexLocation == END
			writer = out -> {
				data.writeTo(out);
				final NDArray<Range> locations = ShardIndex.locations(index, data);
				final DataBlock<long[]> indexDataBlock = ShardIndex.toDataBlock(locations, 0);
				final ReadData indexReadData = indexCodec.encode(indexDataBlock);
				indexReadData.writeTo(out);
			};
		}
		return ReadData.from(writer);
	}

	@Override
	public DataBlock<RawShard> decode(final ReadData readData, final long[] gridPosition) throws N5Exception.N5IOException {

		final long indexOffset = (indexLocation == START) ? 0 : (readData.requireLength() - indexBlockSizeInBytes);
		final ReadData indexReadData = readData.slice(indexOffset, indexBlockSizeInBytes);
		final DataBlock<long[]> indexDataBlock = indexCodec.decode(indexReadData, new long[size.length]);
		final NDArray<Range> locations = ShardIndex.fromDataBlock(indexDataBlock);
		final ShardIndex.SegmentIndexAndData segments = ShardIndex.segments(locations, readData);
		return new RawShardDataBlock(gridPosition, new RawShard(segments));
	}
}
