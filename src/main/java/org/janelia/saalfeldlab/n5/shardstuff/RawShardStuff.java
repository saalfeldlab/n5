package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.List;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedPosition;

public class RawShardStuff {



	public interface RawShard extends DataBlock<Void> {

		// pos is relative to this shard
		ReadData getElementData(long[] pos);

		// pos is relative to this shard
		void setElementData(ReadData data, long[] pos);

		// TODO: removeElement(long[] pos)
	}

	static class BasicRawShard implements RawShard {

		BasicRawShard(final ReadData data, final int[] size) {

			// TODO:
			//  [ ] read shard index (just assume some defaults)
			//  [ ] construct SegmentedReadData
			//  [ ] construct Map<Position, Segment> or flattened Segment[]
			//  [ ] implement setElementData
			//  [ ] implement getElementData

		}

		// --- RawShard ---

		@Override
		public ReadData getElementData(final long[] pos) {
			// TODO
			throw new UnsupportedOperationException();
		}

		@Override
		public void setElementData(final ReadData data, final long[] pos) {
			// TODO
			throw new UnsupportedOperationException();
		}



		// --- DataBlock<Void> ---

		@Override
		public int[] getSize() {
			// TODO
			throw new UnsupportedOperationException();
		}

		@Override
		public long[] getGridPosition() {
			// TODO
			throw new UnsupportedOperationException();
		}

		@Override
		public int getNumElements() {
			// TODO
			throw new UnsupportedOperationException();
		}

		@Override
		public Void getData() {
			// TODO
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

		BasicRawShardCodec(final int[] size) {
			this.size = size;
		}

		@Override
		public ReadData encode(final DataBlock<Void> dataBlock) throws N5IOException {
			// TODO
			throw new UnsupportedOperationException();
		}

		@Override
		public RawShard decode(final ReadData readData, final long[] gridPosition) throws N5IOException {
			// TODO
			throw new UnsupportedOperationException();
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
