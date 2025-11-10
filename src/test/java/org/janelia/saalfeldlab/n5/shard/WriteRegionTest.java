package org.janelia.saalfeldlab.n5.shard;

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.N5Writer.DataBlockSupplier;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;

public class WriteRegionTest {


//	#...............................#...............................#...............................#...............................#
//	$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$
//	|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|
//	0   3   6   9  12  15  18  21  24  27  30  33  36  39  42  45  48  51  54  57  60  63  66  69  72  75  78  81  84  87  90  93  96

	public static void main(String[] args) {

//		int[] datablockSize = {3, 3, 3};
//		int[] level1ShardSize = {6, 6, 6};
//		int[] level2ShardSize = {24, 24, 24};
		int[] datablockSize = {3};
		int[] level1ShardSize = {6};
		int[] level2ShardSize = {24};

		// DataBlocks are 3x3x3
		// Level 1 shards are 6x6x6 (contain 2x2x2 DataBlocks)
		// Level 2 shards are 24x24x24 (contain 4x4x4 Level 1 shards)
		final BlockCodecInfo c0 = new N5BlockCodecInfo();
		final ShardCodecInfo c1 = new DefaultShardCodecInfo(
				datablockSize,
				c0,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.END
		);
		final ShardCodecInfo c2 = new DefaultShardCodecInfo(
				level1ShardSize,
				c1,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.START
		);

		TestDatasetAttributes attributes = new TestDatasetAttributes(
				new long[] {},
				level2ShardSize,
				DataType.INT8,
				c2,
				new RawCompression());

		final DatasetAccess<byte[]> datasetAccess = attributes.datasetAccess();
		final PositionValueAccess store = new TestPositionValueAccess();

		// ---------------------------------------------------------------
		// Some "tests"
		// TODO: Turn into unit tests
		// ---------------------------------------------------------------

		// write some blocks, filled with constant values
		final int[] dataBlockSize = c1.getInnerBlockSize();
//		final long[] datasetDimensions = {100, 100, 100};
//		final long[] regionMin = {9,9,9};
//		final long[] regionSize = {15,15,15};
		final long[] datasetDimensions = {96};

		//	#...............................#...............................#...............................#...............................#
		//	$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$
		//	|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|
		//	0   3   6   9  12  15  18  21  24  27  30  33  36  39  42  45  48  51  54  57  60  63  66  69  72  75  78  81  84  87  90  93  96

		final long[] regionMin = {93};
		final long[] regionSize = {1};

		DataBlockSupplier<byte[]> blocks = (gridPos, existing) -> {
//			System.out.println("BlockSupplier.get(" + Arrays.toString(gridPos) + ", " + existing + ")");
			return createDataBlock(dataBlockSize, gridPos.clone(), (byte) gridPos[0]);
		};
		datasetAccess.writeRegion(store,
				regionMin,
				regionSize,
				blocks,
				datasetDimensions,
				false);

		// verify that the written blocks can be read back with the correct values
//		checkBlock(datasetAccess.readBlock(store, new long[] {0, 0, 0}), true, 1);
//		checkBlock(datasetAccess.readBlock(store, new long[] {1, 0, 0}), true, 2);
//		checkBlock(datasetAccess.readBlock(store, new long[] {0, 1, 0}), true, 3);
//		checkBlock(datasetAccess.readBlock(store, new long[] {1, 1, 0}), true, 4);
//		checkBlock(datasetAccess.readBlock(store, new long[] {3, 2, 1}), true, 5);
//		checkBlock(datasetAccess.readBlock(store, new long[] {8, 4, 1}), true, 6);

		// verify that deleting a block removes it from the shard (while other blocks in the same shard are still present)
//		datasetAccess.deleteBlock(store, new long[] {0, 0, 0});
//		checkBlock(datasetAccess.readBlock(store, new long[] {0, 0, 0}), false, 1);
//		checkBlock(datasetAccess.readBlock(store, new long[] {1, 0, 0}), true, 2);

		// if a shard becomes empty the corresponding key should be deleted
//		if ( store.get(new long[] {1, 0, 0}) == null ) {
//			throw new IllegalStateException("expected non-null readData");
//		}
//		datasetAccess.deleteBlock(store, new long[] {8, 4, 1});
//		if ( store.get(new long[] {1, 0, 0}) != null ) {
//			throw new IllegalStateException("expected null readData");
//		}

		// deleting a non-existent block should not fail
//		datasetAccess.deleteBlock(store, new long[] {0, 0, 8});

		System.out.println("all good");
	}

	private static void checkBlock(final DataBlock<byte[]> dataBlock, final boolean expectedNonNull, final int expectedFillValue) {

		if (dataBlock == null) {
			if (expectedNonNull) {
				throw new IllegalStateException("expected non-null dataBlock");
			}
		} else {
			if (!expectedNonNull) {
				throw new IllegalStateException("expected null dataBlock");
			}
			final byte[] bytes = dataBlock.getData();
			for (byte b : bytes) {
				if (b != (byte) expectedFillValue) {
					throw new IllegalStateException("expected all values to be " + expectedFillValue);
				}
			}
		}
	}

	private static DataBlock<byte[]> createDataBlock(int[] size, long[] gridPosition, int fillValue) {
		final byte[] bytes = new byte[DataBlock.getNumElements(size)];
		Arrays.fill(bytes, (byte) fillValue);
		return new ByteArrayDataBlock(size, gridPosition, bytes);
	}

	public static class TestDatasetAttributes extends DatasetAttributes {

		public TestDatasetAttributes(long[] dimensions, int[] outerBlockSize, DataType dataType, BlockCodecInfo blockCodecInfo,
				DataCodecInfo... dataCodecInfos) {

			super(dimensions, outerBlockSize, dataType, blockCodecInfo, dataCodecInfos);
		}

		public DatasetAccess datasetAccess() {

			// to make this accessible for the test
			return createDatasetAccess();
		}

	}

}
