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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.commons.lang3.stream.Streams;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;
import org.junit.Test;

public class WriteShardTest {


//	#...............................#...............................#...............................#...............................#
//	$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$.......$
//	|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|...|
//	0   3   6   9  12  15  18  21  24  27  30  33  36  39  42  45  48  51  54  57  60  63  66  69  72  75  78  81  84  87  90  93  96

	public static void main(String[] args) {

		final int[] datablockSize = {3};
		final int[] level1ShardSize = {6};
		final long[] datasetDimensions = {36};

		// DataBlocks are 3
		// Level 1 shards are 6
		final BlockCodecInfo c0 = new N5BlockCodecInfo();
		final ShardCodecInfo c1 = new DefaultShardCodecInfo(
				datablockSize,
				c0,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.END
		);

		TestDatasetAttributes attributes = new TestDatasetAttributes(
				datasetDimensions,
				level1ShardSize,
				DataType.INT32,
				c1,
				new RawCompression());

		final DatasetAccess<int[]> datasetAccess = attributes.datasetAccess();
		final PositionValueAccess store = new TestPositionValueAccess();

		//	0       1       2       3       4       5       6
		//	$.......$.......$.......$.......$.......$.......$
		//	0   1   2   3   4   5   6   7   8   9  10  11  12
		//	|...|...|...|...|...|...|...|...|...|...|...|...|
		//	0   3   6   9  12  15  18  21  24  27  30  33  36
		//  .................................................

		final int[] dataBlockSize = c1.getInnerBlockSize();

		// create a shard DataBlock
		{
			final DataBlock<int[]> shard = createDataBlock(level1ShardSize, new long[] {2}, 1);
			System.out.println("shard.getGridPosition() = " + Arrays.toString(shard.getGridPosition()));
			System.out.println("shard.getSize() = " + Arrays.toString(shard.getSize()));
			System.out.println("shard.getData() = " + Arrays.toString(shard.getData()));
			System.out.println();
			datasetAccess.writeShard(store, shard, 1);
			System.out.println();
			System.out.println();
		}
		{
			final DataBlock<int[]> shard = createDataBlock(level1ShardSize, new long[] {5}, 1);
			System.out.println("shard.getGridPosition() = " + Arrays.toString(shard.getGridPosition()));
			System.out.println("shard.getSize() = " + Arrays.toString(shard.getSize()));
			System.out.println("shard.getData() = " + Arrays.toString(shard.getData()));
			System.out.println();
			datasetAccess.writeShard(store, shard, 1);
			System.out.println();
			System.out.println();
		}



		System.out.println("all good");
	}
	
	@Test
	public void testShardDatasetAccess() {

		final int[] datablockSize = {3};
		final int[] level1ShardSize = {6};
		final long[] datasetDimensions = {36};

		// DataBlocks are 3
		// Level 1 shards are 6
		final BlockCodecInfo c0 = new N5BlockCodecInfo();
		final ShardCodecInfo c1 = new DefaultShardCodecInfo(
				datablockSize,
				c0,
				new DataCodecInfo[] {new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[] {new RawCompression()},
				IndexLocation.END
		);

		TestDatasetAttributes attributes = new TestDatasetAttributes(
				datasetDimensions,
				level1ShardSize,
				DataType.INT32,
				c1,
				new RawCompression());

		final DatasetAccess<int[]> datasetAccess = attributes.datasetAccess();
		final PositionValueAccess store = new TestPositionValueAccess();

		//	0       1       2       3       4       5       6
		//	$.......$.......$.......$.......$.......$.......$
		//	0   1   2   3   4   5   6   7   8   9  10  11  12
		//	|...|...|...|...|...|...|...|...|...|...|...|...|
		//	0   3   6   9  12  15  18  21  24  27  30  33  36
		//  .................................................

		long[] p = {0};
		assertFalse(store.exists(p));
	}

	@Test
	public void testWriteNullBlockRemovesShard() throws Exception {

		final int[] datablockSize = {3};
		final int[] level1ShardSize = {6};
		final long[] datasetDimensions = {36};

		// Level 1 shards have size 6 (each containing two datablocks of size 3)
		final BlockCodecInfo c0 = new N5BlockCodecInfo();
		final ShardCodecInfo c1 = new DefaultShardCodecInfo(
				datablockSize,
				c0,
				new DataCodecInfo[]{new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[]{new RawCompression()},
				IndexLocation.END);

		TestDatasetAttributes attributes = new TestDatasetAttributes(
				datasetDimensions,
				level1ShardSize,
				DataType.INT32,
				c1,
				new RawCompression());

		final DatasetAccess<int[]> datasetAccess = attributes.datasetAccess();
		final PositionValueAccess store = new TestPositionValueAccess();
		final long[] shardKey = {1};

		/**
		 * ONE BLOCK, ONE SHARD
		 */

		assertFalse("Shard should not exist at the start of the test", store.exists(shardKey));

		// Write a single block at grid position [3]
		// This block is in shard [1] 
		// ( shard 0 contains blocks 0-1,
		//   shard 1 contains blocks 2-3 )
		final long[] blockGridPosition = {3};
		final DataBlock<int[]> block = createDataBlock(datablockSize, blockGridPosition, 100);

		datasetAccess.writeBlock(store, block);

		// Verify the shard exists
		assertTrue("Shard should exist after writing block", store.exists(shardKey));

		// Write a null block at the same location using writeRegion
		// This should remove the block and delete the now-empty shard
		final long[] regionMin = {9}; // pixel position of block [3]
		final long[] regionSize = {3}; // size of one block

		datasetAccess.writeRegion(
				store,
				regionMin,
				regionSize,
				(gridPosition, existingBlock) -> null, // block supplier returns
														// null to indicate
														// removal
				false);

		// Verify the shard has been removed
		assertFalse("Shard should be removed after writing null block", store.exists(shardKey));
		
		/**
		 * THREE BLOCKS, TWO SHARDS
		 */
		// Write two blocks into the same shard, and one block into a second shard
		// Shard [1] contains blocks [2] and [3]
		// Shard [2] contains block  [4]
		final long[] shardKey1 = {1};
		final long[] shardKey2 = {2};

		final DataBlock<int[]> block1 = createDataBlock(datablockSize, new long[]{2}, 100);
		final DataBlock<int[]> block2 = createDataBlock(datablockSize, new long[]{3}, 200);
		final DataBlock<int[]> block3 = createDataBlock(datablockSize, new long[]{4}, 300);


		assertFalse("Shard should not exist at the start of the test", store.exists(shardKey1));
		assertFalse("Shard should not exist at the start of the test", store.exists(shardKey2));

		// write blocks
		datasetAccess.writeBlocks(store, Streams.of(block1, block2, block3).collect(Collectors.toList()));

		// Verify the shard exists
		assertTrue("Shard should exist after writing blocks", store.exists(shardKey1));
		assertTrue("Shard should exist after writing blocks", store.exists(shardKey2));

		// Write a null block at block [3]'s location
		datasetAccess.writeRegion(
				store,
				regionMin,
				regionSize,
				(gridPosition, existingBlock) -> null,
				false);

		// Verify the shard still exists (because block [2] is still there)
		assertTrue("Shard should still exist because it contains another block", store.exists(shardKey1));
		assertTrue("Shard should still exist because was not affected", store.exists(shardKey2));

		// Verify we can still read block [2]
		final DataBlock<int[]> readBlock = datasetAccess.readBlock(store, new long[]{2});
		assertTrue("Block [2] should still be readable", readBlock != null);
		assertTrue("Block [2] data should match", Arrays.equals(block1.getData(), readBlock.getData()));

		// Verify block [3] is gone
		final DataBlock<int[]> readBlock2 = datasetAccess.readBlock(store, new long[]{3});
		assertNull("Block [3] should be null after removal", readBlock2);

		// Verify block [4] exists
		final DataBlock<int[]> readBlock3 = datasetAccess.readBlock(store, new long[]{4});
		assertTrue("Block [4] should still be readable", readBlock3 != null);
		assertTrue("Block [4] data should match", Arrays.equals(block3.getData(), readBlock3.getData()));

		// Write a null block at block [2]'s location
		final long[] regionMin2 = {6}; // pixel position of block [3]
		final long[] regionSize2 = {3};

		datasetAccess.writeRegion(
				store,
				regionMin2,
				regionSize2,
				(gridPosition, existingBlock) -> null,
				false);

		assertFalse("Shard should not exist after deleting all blocks", store.exists(shardKey1));
		assertTrue("Shard should still exist because was not affected", store.exists(shardKey2));
	}

	private static DataBlock<int[]> createDataBlock(int[] size, long[] gridPosition, int startValue) {
		final int[] ints = new int[DataBlock.getNumElements(size)];
		Arrays.setAll(ints, i -> i + startValue);
		return new IntArrayDataBlock(size, gridPosition, ints);
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
