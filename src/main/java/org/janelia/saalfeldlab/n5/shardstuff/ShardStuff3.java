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

public class ShardStuff3 {



	public interface RawShard extends DataBlock<ReadData> {

		// pos is relative to this shard
		ReadData getElementData(long[] pos);

		// pos is relative to this shard
		void setElementData(ReadData data, long[] pos);

		// TODO: removeElement(long[] pos)
	}

	static class BasicRawShard {

		BasicRawShard(final ReadData data, final int[] size) {

			// TODO:
			//  [ ] read shard index (just assume some defaults)
			//  [ ] construct SegmentedReadData
			//  [ ] construct Map<Position, Segment> or flattened Segment[]
			//  [ ] implement setElementData
			//  [ ] implement getElementData

		}



	}






	public interface RawShardCodec extends BlockCodec<ReadData> {

		@Override
		RawShard decode(ReadData readData, long[] gridPosition) throws N5IOException;
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
