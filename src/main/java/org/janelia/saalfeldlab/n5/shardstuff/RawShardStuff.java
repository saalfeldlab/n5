package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.List;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.readdata.segment.Segment;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentLocation;
import org.janelia.saalfeldlab.n5.readdata.segment.SegmentedReadData;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedPosition;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.NDArray;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation;
import org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.SegmentIndexAndData;

import static org.janelia.saalfeldlab.n5.shardstuff.ShardIndex.IndexLocation.START;

public class RawShardStuff {



	public interface RawShard extends DataBlock<Void> {

		// pos is relative to this shard
		ReadData getElementData(long[] pos);

		// pos is relative to this shard
		void setElementData(ReadData data, long[] pos);

		// TODO: removeElement(long[] pos)
	}

	static class BasicRawShard implements RawShard {

		private long[] gridPosition;

		/**
		 * The ReadData from which the shard was constructed, or {@code null}
		 * for a new empty shard.
		 */
		private SegmentedReadData sourceData;

		/**
		 * maps grid position of shard elements to {@link Segment}s.
		 */
		private NDArray<Segment> index;

		BasicRawShard(final int[] size) {
			sourceData = null;
			index = new NDArray<>(size, Segment[]::new);
		}

		BasicRawShard(final long[] gridPosition, final SegmentedReadData sourceData, final NDArray<Segment> index) {
			this.gridPosition = gridPosition;
			this.sourceData = sourceData;
			this.index = index;
		}


		// --- RawShard ---

		@Override
		public ReadData getElementData(final long[] pos) {
			final Segment segment = index.get(pos);
			return segment == null ? null : segment.source().slice(segment);
		}

		@Override
		public void setElementData(final ReadData data, final long[] pos) {
			final Segment segment = SegmentedReadData.wrap(data).segments().getFirst();
			index.set(segment, pos);
		}


		// --- DataBlock<Void> ---

		@Override
		public int[] getSize() {
			return index.size();
		}

		@Override
		public long[] getGridPosition() {
			return gridPosition;
		}

		@Override
		public int getNumElements() {
			return index.numElements();
		}

		@Override
		public Void getData() {
			return null;
		}
	}






	public interface RawShardCodec extends BlockCodec<Void> {

		@Override
		RawShard decode(ReadData readData, long[] gridPosition) throws N5IOException;
	}

	static class BasicRawShardCodec implements RawShardCodec {

		/**
		 * Number of elements (DataBlocks, nested shards) in each dimension per shard.
		 */
		private final int[] size;
		private final IndexLocation indexLocation;
		private final BlockCodec<long[]> indexCodec;
		private final long indexBlockSizeInBytes;

		BasicRawShardCodec(final int[] size, final IndexLocation indexLocation, final BlockCodec<long[]> indexCodec) {

			this.size = size;
			this.indexLocation = indexLocation;
			this.indexCodec = indexCodec;
			indexBlockSizeInBytes = indexCodec.encodedSize(ShardIndex.blockSizeFromIndexSize(size));
		}

		@Override
		public ReadData encode(final DataBlock<Void> dataBlock) throws N5IOException {
			// TODO
			throw new UnsupportedOperationException();
		}

		@Override
		public RawShard decode(final ReadData readData, final long[] gridPosition) throws N5IOException {

			final long indexOffset = (indexLocation == START) ? 0 : (readData.requireLength() - indexBlockSizeInBytes);
			final ReadData indexReadData = readData.slice(indexOffset, indexBlockSizeInBytes);
			final DataBlock<long[]> indexDataBlock = indexCodec.decode(indexReadData, new long[size.length]);
			final NDArray<SegmentLocation> locations = ShardIndex.fromDataBlock(indexDataBlock);
			final SegmentIndexAndData segments = ShardIndex.segments(locations, readData);
			return new BasicRawShard(gridPosition, segments.data(), segments.index());
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
