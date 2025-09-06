package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.ArrayList;
import java.util.List;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.ReadData.OutputStreamWriter;
import org.janelia.saalfeldlab.n5.readdata.segment.Segment;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentLocation;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedPosition;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.NDArray;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.SegmentIndexAndData;

import static org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation.START;

public class RawShardStuff {


	public static class RawShardData {

		private final SegmentedReadData sourceData;

		private final NDArray<Segment> index;

		RawShardData(final int[] size) {
			sourceData = null;
			index = new NDArray<>(size, Segment[]::new);
		}

		RawShardData(final SegmentedReadData sourceData, final NDArray<Segment> index) {
			this.sourceData = sourceData;
			this.index = index;
		}

		RawShardData(final SegmentIndexAndData segmentIndexAndData) {
			this(segmentIndexAndData.data(), segmentIndexAndData.index());
		}


		/**
		 * The ReadData from which the shard was constructed, or {@code null}
		 * for a new empty shard.
		 */
		public SegmentedReadData sourceData() {
			return sourceData;
		}

		/**
		 * Maps grid position of shard elements to {@link Segment}s.
		 */
		public NDArray<Segment> index() {
			return index;
		}
	}


	public static class RawShard implements DataBlock<RawShardData> {

		private final long[] gridPosition;

		private final RawShardData shardData;

		RawShard(final long[] gridPosition, final RawShardData shardData) {
			this.gridPosition = gridPosition;
			this.shardData = shardData;
		}

		public ReadData getElementData(final long[] pos) {
			final Segment segment = shardData.index().get(pos);
			return segment == null ? null : segment.source().slice(segment);
		}

		public void setElementData(final ReadData data, final long[] pos) {
			final Segment segment = SegmentedReadData.wrap(data).segments().getFirst();
			shardData.index().set(segment, pos);
		}

		// --- DataBlock<RawShardData> ---

		@Override
		public int[] getSize() {
			return shardData.index().size();
		}

		@Override
		public long[] getGridPosition() {
			return gridPosition;
		}

		@Override
		public int getNumElements() {
			return shardData.index().numElements();
		}

		@Override
		public RawShardData getData() {
			return shardData;
		}
	}


	public static class RawShardCodec implements BlockCodec<RawShardData> {

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
		public ReadData encode(final DataBlock<RawShardData> shard) throws N5IOException {

			// concatenate slices for all non-null segments in shard.getData().index()
			final NDArray<Segment> index = shard.getData().index();
			final List<SegmentedReadData> readDatas = new ArrayList<>();
			for (Segment segment : index.data) {
				if (segment != null ) {
					readDatas.add(segment.source().slice(segment));
				}
			}
			final SegmentedReadData data = SegmentedReadData.concatenate(readDatas);
			final OutputStreamWriter writer;
			if (indexLocation == START) {
				data.materialize();
				final NDArray<SegmentLocation> locations = ShardIndex.locations(index, data);
				final DataBlock<long[]> indexDataBlock = ShardIndex.toDataBlock(locations, indexBlockSizeInBytes);
				final ReadData indexReadData = indexCodec.encode(indexDataBlock);
				writer = out -> {
					indexReadData.writeTo(out);
					data.writeTo(out);
				};
			} else { // indexLocation == END
				writer = out -> {
					data.writeTo(out);
					final NDArray<SegmentLocation> locations = ShardIndex.locations(index, data);
					final DataBlock<long[]> indexDataBlock = ShardIndex.toDataBlock(locations, 0);
					final ReadData indexReadData = indexCodec.encode(indexDataBlock);
					indexReadData.writeTo(out);
				};
			}
			return ReadData.from(writer);
		}

		@Override
		public RawShard decode(final ReadData readData, final long[] gridPosition) throws N5IOException {

			final long indexOffset = (indexLocation == START) ? 0 : (readData.requireLength() - indexBlockSizeInBytes);
			final ReadData indexReadData = readData.slice(indexOffset, indexBlockSizeInBytes);
			final DataBlock<long[]> indexDataBlock = indexCodec.decode(indexReadData, new long[size.length]);
			final NDArray<SegmentLocation> locations = ShardIndex.fromDataBlock(indexDataBlock);
			final SegmentIndexAndData segments = ShardIndex.segments(locations, readData);
			return new RawShard(gridPosition, new RawShardData(segments));
		}
	}





	// TODO: rename?
	public interface Sharding<T> {

		List<DataBlock<T>> readBlocks(ReadData readData, List<NestedPosition> positions);

		ReadData writeBlocks(ReadData readData, List<DataBlock<T>> blocks);
	}

	public interface ShardCodecInfo extends CodecInfo {
		/**
		 * Chunk size of the elements in this block.
		 * That is, (1, ...) for DataBlockCodecInfo, respectively inner block size for ShardCodecInfo.
		 */
		int[] getInnerBlockSize();

		/**
		 * Nested BlockCodec.
		 * ({@code null} for DataBlockCodec.
		 */
		BlockCodecInfo getInnerBlockCodecInfo();

		DataCodecInfo[] getInnerDataCodecInfos();

		// TODO: IndexCodec

		RawShardCodec createRaw(int[] blockSize, DataCodecInfo... codecs);

		// TODO: not sure about this one.
		//  This could recursively built the whole codec list?
		//  The result would have to be a BlockCodec<T>.
		//
//		??? create(DataType dataType, int[] blockSize, DataCodecInfo... codecs) {}
	}
}
