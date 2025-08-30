package org.janelia.saalfeldlab.n5.shardstuff;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
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


	public interface BlockCodecInfo extends CodecInfo {
		// when moving to Java 17+:
		// this should be sealed, permitting ShardCodecInfo and DataBlockCodecInfo
	}

	public interface ShardCodecInfo extends BlockCodecInfo {

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

		ShardCodec create(int[] blockSize, DataCodecInfo... codecs);
	}

	public interface DataBlockCodecInfo extends BlockCodecInfo {
		<T> DataBlockCodec<T> create(DataType dataType, int[] blockSize, DataCodecInfo... codecs);
	}




	// -----------------
	// --- "testing" ---

	public static void main(String[] args) {

		// DataBlocks are 3x3x3
		// Level 1 shards are 6x6x6 (contain 2x2x2 DataBlocks)
		// Level 2 shards are 24x24x24 (contain 4x4x4 Level 1 shards)

		final DataBlockCodecInfo c0 = new DummyDataBlockCodecInfo();
		final ShardCodecInfo c1 = new DummyShardCodecInfo(
				new int[] {3, 3, 3},
				c0,
				new DataCodecInfo[] {new RawCompression()}
		);
		final ShardCodecInfo c2 = new DummyShardCodecInfo(
				new int[] {6, 6, 6},
				c1,
				new DataCodecInfo[] {new RawCompression()}
		);

		final WipShardingCodec<Object> wipShardingCodec = create(DataType.INT8,
				new int[] {24, 24, 24},
				c2,
				new DataCodecInfo[] {new RawCompression()});

		System.out.println("wipShardingCodec = " + wipShardingCodec);
	}

	public static class DummyShardCodecInfo implements ShardCodecInfo {

		private final int[] innerBlockSize;
		private final BlockCodecInfo innerBlockCodecInfo;
		private final DataCodecInfo[] innerDataCodecInfos;

		public DummyShardCodecInfo(
				final int[] innerBlockSize,
				final BlockCodecInfo innerBlockCodecInfo,
				final DataCodecInfo[] innerDataCodecInfos) {
			this.innerBlockSize = innerBlockSize;
			this.innerBlockCodecInfo = innerBlockCodecInfo;
			this.innerDataCodecInfos = innerDataCodecInfos;
		}

		@Override
		public int[] getInnerBlockSize() {
			return innerBlockSize;
		}

		@Override
		public BlockCodecInfo getInnerBlockCodecInfo() {
			return innerBlockCodecInfo;
		}

		@Override
		public DataCodecInfo[] getInnerDataCodecInfos() {
			return innerDataCodecInfos;
		}

		@Override
		public ShardCodec create(final int[] blockSize, final DataCodecInfo... codecs) {
			return null;
		}

		@Override
		public String getType() {
			return "DummyShardCodec";
		}
	}

	public static class DummyDataBlockCodecInfo implements DataBlockCodecInfo {

		@Override
		public <T> DataBlockCodec<T> create(final DataType dataType, final int[] blockSize, final DataCodecInfo... codecs) {
			return null;
		}

		@Override
		public String getType() {
			return "DummyDataBlockCodecInfo";
		}
	}

	// --- "testing" ---
	// -----------------











	// NB: Something needs to get the ReadData from the KVA.
	//     We cannot put readBlocks(...) logic into the (Data)BlockCodec, because that doesn't have access to the KVA.
	//     We could put readBlocks(ReadData, ...) into the ShardCodec, to read several blocks from the same Shard.
	//     Then the N5Reader only has to do one level of batching.

	// TODO: Consider making this really recursive.
	//       ... but postpone until the overall shape is a bit clearer.

	static class WipShardingCodec<T> {

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
		public DataBlock<T> decodeNested(final ReadData readData, final NestedPosition position) throws N5IOException {

			// NB: Assuming position.level==0 (refers to a DataBlock) and has
			// nesting corresponding (at least) to our grid. This should perhaps
			// be verified, at least during development.

			if ( nestedCodec != null ) {
				final int l = shardCodecs.length - 1;
				final Shard shard = shardCodecs[l].decode(readData, position.absolutePosition(l));
				final ReadData nestedData = shard.getElementData(position.relativePosition(l - 1));
				return nestedCodec.decodeNested(nestedData, position);
			} else {
				return dataBlockCodec.decode(readData, position.absolutePosition(0));
			}
		}




		// --- DataBlockCodec<T> ---

//		@Override
//		public ReadData encode(final DataBlock<T> block) throws N5IOException {
//		}
//
//		@Override
//		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws N5IOException {
//		}
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
			nestedWipShardingCodec = create(dataType, info.getInnerBlockSize(), info.getInnerBlockCodecInfo(), info.getInnerDataCodecInfos());
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
			blockSize = info.getInnerBlockSize();
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
