package org.janelia.saalfeldlab.n5.shardstuff;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.GsonKeyValueN5Reader;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5URI;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.readdata.ReadData;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shardstuff.Nesting.NestedPosition;

public class ShardStuff2 {

	public interface Block {

		/**
		 * Returns the number of elements in a box of given size.
		 *
		 * @param size
		 * 		the size
		 *
		 * @return the number of elements
		 */
		static int getNumElements(final int[] size) {

			int n = size[0];
			for (int i = 1; i < size.length; ++i)
				n *= size[i];
			return n;
		}

		/**
		 * Returns the size of this block.
		 * <p>
		 * The size of a data block is expected to be smaller than or equal to the
		 * spacing of the block grid. The dimensionality of size is expected to be
		 * equal to the dimensionality of the dataset. Consistency is not enforced.
		 *
		 * @return size of the block
		 */
		int[] getSize();

		/**
		 * Returns the grid position of this block.
		 * <p>
		 * Grid position is with respect to the level of the block (Shard, nested Shard, DataBlock, ...).
		 * If a block has grid position (i, ...) the adjacent block (in X) has position (i+1, ...).
		 * Grid position (0, ...) is the origin of the dataset, that is, grid positions are not relative to the containing shard etc.
		 * <p>
		 * The dimensionality of the grid position is expected to be equal to the
		 * dimensionality of the dataset. Consistency is not enforced.
		 *
		 * @return position on the block grid
		 */
		long[] getGridPosition();
	}


	public interface Shard extends Block {

		// pos is relative to this shard
		ReadData getElementData(long[] pos);

		// pos is relative to this shard
		void setElementData(ReadData data, long[] pos);

		// TODO: removeElement(long[] pos)
	}


	public interface DataBlock<T> extends Block {

		/**
		 * Returns the data object held by this data block.
		 *
		 * @return data object
		 */
		T getData();

//		interface DataBlockFactory<T> {
//			DataBlock<T> createDataBlock(int[] blockSize, long[] gridPosition, T data);
//		}
	}







	public interface BlockCodec<B extends Block> {

		ReadData encode(B block) throws N5IOException;

		/**
		 * {@code gridPosition} is with respect to the level of the block (Shard, nested Shard, DataBlock, ...).
		 * If a block has gridPosition (i, ...), then the adjacent block (in X) has position (i+1, ...).
		 * Grid position (0, ...) is the origin of the dataset, that is, grid positions are not relative to the containing shard etc.
		 */
		B decode(ReadData readData, long[] gridPosition) throws N5IOException;
	}

	public interface ShardCodec extends BlockCodec<Shard> {
	}

	public interface DataBlockCodec<T> extends BlockCodec<DataBlock<T>> {
	}



	public interface ShardCodecInfo extends BlockCodecInfo {

		/**
		 * Chunk size of the elements in this block.
		 * That is, (1, ...) for DataBlockCodecInfo, respectively inner block size for ShardCodecInfo.
		 */
		int[] getChunkSize();

		/**
		 * Nested BlockCodec.
		 * ({@code null} for DataBlockCodec.
		 */
		BlockCodecInfo getInnerBlockCodecInfo();

		DataCodecInfo[] getInnerDataCodecInfos();

		// TODO: IndexCodec

		ShardCodec create(int[] blockSize, DataCodecInfo... codecs);
	}

	public interface DataBlockCodecInfo extends BlockCodecInfo {
		<T> DataBlockCodec<T> create(DataType dataType, int[] blockSize, DataCodecInfo... codecs);
	}



	// NB: Something needs to get the ReadData from the KVA.
	//     We cannot put readBlocks(...) logic into the (Data)BlockCodec, because that doesn't have access to the KVA.
	//     We could put readBlocks(ReadData, ...) into the ShardCodec, to read several blocks from the same Shard.
	//     Then the N5Reader only has to do one level of batching.

	// TODO: Consider making this really recursive.
	//       ... but postpone until the overall shape is a bit clearer.

	static class WipShardingCodec<T> implements DataBlockCodec<T> {

		private final NestedGrid grid;
		private final DataBlockCodec<T> dataBlockCodec;
		private final ShardCodec[] shardCodecs;

		private final WipShardingCodec<T> nestedCodec; // TODO: For exploring logic options, we create a nested WipShardingCodec.

		public WipShardingCodec(final NestedGrid grid, final DataBlockCodec<T> dataBlockCodec, final ShardCodec[] shardCodecs, final WipShardingCodec<T> nestedCodec) {
			this.grid = grid;
			this.dataBlockCodec = dataBlockCodec;
			this.shardCodecs = shardCodecs;
			this.nestedCodec = nestedCodec;
		}

		public NestedPosition toNestedPosition(final long[] gridPosition) {
			return new NestedPosition(grid, gridPosition);
		}

		// TODO: implementation without nestedCodec
		public DataBlock<T> decode(ReadData readData, final NestedPosition position) throws N5IOException {

			// NB: Assuming position.level==0 (refers to a DataBlock) and has
			// nesting corresponding (at least) to our grid. This should perhaps
			// be verified, at least during development.

			for ( int l = grid.numLevels() - 1; l > 0; --l ) {
				final Shard shard = shardCodecs[l].decode(readData, position.absolutePosition(l));
				readData = shard.getElementData(position.relativePosition(l - 1));
			}
			return dataBlockCodec.decode(readData, position.absolutePosition(0));
		}

		// TODO: alternative implementation that uses nestedCodec
		public DataBlock<T> decode2(final ReadData readData, final NestedPosition position) throws N5IOException {

			// NB: Assuming position.level==0 (refers to a DataBlock) and has
			// nesting corresponding (at least) to our grid. This should perhaps
			// be verified, at least during development.

			if ( nestedCodec != null ) {
				final int l = shardCodecs.length - 1;
				final Shard shard = shardCodecs[l].decode(readData, position.absolutePosition(l));
				final ReadData nestedData = shard.getElementData(position.relativePosition(l - 1));
				return nestedCodec.decode(nestedData, position);
			} else {
				return dataBlockCodec.decode(readData, position.absolutePosition(0));
			}
		}





		@Override
		public ReadData encode(final DataBlock<T> block) throws N5IOException {
			throw new UnsupportedOperationException("TODO");
		}

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws N5IOException {
			throw new UnsupportedOperationException("TODO");
		}
	}

	static <T> WipShardingCodec<T> create(
			final DataType dataType,
			int[] blockSize,
			BlockCodecInfo blockCodecInfo,
			DataCodecInfo[] dataCodecInfos
	) {
		final int m = nestingDepth(blockCodecInfo);

		// TODO: For exploring logic options, we create a nested WipShardingCodec.
		//  If that turns out to be useful, the below logic should be simplified
		//  to re-use info from the nested WipShardingCodec.
		final WipShardingCodec<T> nestedWipShardingCodec;
		if ( m > 1 ) {
			final ShardCodecInfo info = (ShardCodecInfo) blockCodecInfo;
			nestedWipShardingCodec = create(dataType, info.getChunkSize(), info.getInnerBlockCodecInfo(), info.getInnerDataCodecInfos());
		} else {
			nestedWipShardingCodec = null;
		}

		final int[][] blockSizes = new int[m][];

		// There are m codecs: 1 DataBlockCodec, m-1 ShardCodecs.
		// Outermost codec comes last.
		// To make the index math easier, shardCodecs[0] = null (the dataBlockCodec is at level 0).
		final DataBlockCodec<T> dataBlockCodec;
		final ShardCodec[] shardCodecs = new ShardCodec[m];

		for (int l = m - 1; l > 0; --l) {
			final ShardCodecInfo info = (ShardCodecInfo) blockCodecInfo; // TODO instanceof check
			blockSizes[l] = blockSize;
			shardCodecs[l] = info.create(blockSize, dataCodecInfos);
			blockCodecInfo = info.getInnerBlockCodecInfo();
			dataCodecInfos = info.getInnerDataCodecInfos();
			blockSize = info.getChunkSize();
		}

		final DataBlockCodecInfo info = (DataBlockCodecInfo) blockCodecInfo; // TODO instanceof check
		blockSizes[0] = blockSize;
		dataBlockCodec = info.create(dataType, blockSize, dataCodecInfos);

		final NestedGrid grid = new NestedGrid(blockSizes);

		return new WipShardingCodec<>(grid, dataBlockCodec, shardCodecs, nestedWipShardingCodec);
	}

	private static int nestingDepth(BlockCodecInfo info) {
		if (info instanceof ShardCodecInfo) {
			return 1 + nestingDepth(((ShardCodecInfo) info).getInnerBlockCodecInfo());
		} else {
			return 1;
		}
	}
}
