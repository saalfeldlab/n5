package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.shard.DefaultShardCodecInfo;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedGrid;
import org.janelia.saalfeldlab.n5.shard.ShardIndex.IndexLocation;
import org.junit.Test;

/**
 * Unit tests for DatasetAttributes class.
 */
public class DatasetAttributesTest {

	/**
	 * Test that validateBlockShardSizes method accepts valid shard and chunk size combinations.
	 */
	@Test
	public void testValidateBlockShardSizesValid() {

		// Test case 1: shard size equals block size
		long[] dimensions = new long[]{100, 200, 300};
		int[] shardSize = new int[]{64, 64, 64};
		int[] chunkSize = new int[]{64, 64, 64};
		DataType dataType = DataType.UINT8;

		// This should not throw any exception
		DatasetAttributes attrs = shardDatasetAttributes(dimensions, shardSize, chunkSize, dataType);
		assertEquals(chunkSize, attrs.getChunkSize());
		assertEquals(shardSize, attrs.getBlockSize());
		NestedGrid grid = attrs.getNestedBlockGrid();
		assertEquals(chunkSize, grid.getBlockSize(0));
		assertEquals(shardSize, grid.getBlockSize(1));

		// Test case 2: shard size is a multiple of block size
		shardSize = new int[]{128};
		chunkSize = new int[]{64};
		attrs = shardDatasetAttributes(new long[]{128}, shardSize, chunkSize, dataType);
		assertEquals(chunkSize, attrs.getChunkSize());
		assertEquals(shardSize, attrs.getBlockSize());
		grid = attrs.getNestedBlockGrid();
		assertEquals(chunkSize, grid.getBlockSize(0));
		assertEquals(shardSize, grid.getBlockSize(1));

		// Test case 3: different multiples per dimension
		shardSize = new int[]{128, 256, 32, 2};
		chunkSize = new int[]{32, 64, 32, 1};
		attrs = shardDatasetAttributes(new long[]{128, 128, 128, 128}, shardSize, chunkSize, dataType );
		assertEquals(chunkSize, attrs.getChunkSize());
		assertEquals(shardSize, attrs.getBlockSize());
		grid = attrs.getNestedBlockGrid();
		assertEquals(chunkSize, grid.getBlockSize(0));
		assertEquals(shardSize, grid.getBlockSize(1));

		// Test case 4: large multiples
		shardSize = new int[]{1024, 2048, 512};
		chunkSize = new int[]{32, 64, 16};
		attrs = shardDatasetAttributes(dimensions, shardSize, chunkSize, dataType);
		assertEquals(chunkSize, attrs.getChunkSize());
		assertEquals(shardSize, attrs.getBlockSize());
		grid = attrs.getNestedBlockGrid();
		assertEquals(chunkSize, grid.getBlockSize(0));
		assertEquals(shardSize, grid.getBlockSize(1));
	}

	private static DatasetAttributes shardDatasetAttributes(
			long[] dimensions, int[] shardSize, int[] chunkSize, DataType dataType) {

		DefaultShardCodecInfo blockCodecInfo = new DefaultShardCodecInfo(
				chunkSize,
				new N5BlockCodecInfo(),
				new DataCodecInfo[]{new RawCompression()},
				new RawBlockCodecInfo(),
				new DataCodecInfo[]{new RawCompression()},
				IndexLocation.END);

		return new DatasetAttributes(dimensions, shardSize, dataType, blockCodecInfo);
	}

	/**
	 * Test that validateBlockShardSizes method rejects invalid shard and block size combinations.
	 */
	@Test
	public void testValidateBlockShardSizesInvalid() {

		final long[] dimensions = new long[]{100, 200, 300};
		final DataType dataType = DataType.UINT8;

		// Block size too small
		IllegalArgumentException ex0 = assertThrows(
				IllegalArgumentException.class,
				() -> shardDatasetAttributes(dimensions, new int[]{1, 1, 1}, new int[]{1, 0, -1}, dataType));
		assertTrue(ex0.getMessage().contains("negative"));

		// Different number of dimensions
		IllegalArgumentException ex1 = assertThrows(
				IllegalArgumentException.class,
				() -> shardDatasetAttributes(dimensions, new int[]{64, 64}, new int[]{32, 32, 32}, dataType));
		assertTrue(ex1.getMessage().contains("different length"));

		// Shard size smaller than block size
		IllegalArgumentException ex2 = assertThrows(
				IllegalArgumentException.class,
				() -> shardDatasetAttributes(dimensions, new int[]{32, 64, 64}, new int[]{64, 64, 64}, dataType));
		assertTrue(ex2.getMessage().contains("is smaller than previous"));

		// Shard size not a multiple of block size
		IllegalArgumentException ex3 = assertThrows(
				IllegalArgumentException.class,
				() -> shardDatasetAttributes(dimensions, new int[]{100, 100, 100}, new int[]{64, 64, 64}, dataType));
		assertTrue(ex3.getMessage().contains("not a multiple of previous level"));

		// Multiple violations - shard smaller than block in one dimension
		IllegalArgumentException ex4 = assertThrows(
				IllegalArgumentException.class,
				() -> shardDatasetAttributes(dimensions, new int[]{128, 32, 128}, new int[]{64, 64, 64}, dataType));
		assertTrue(ex4.getMessage().contains("is smaller than previous"));

		// Edge case - shard size of 0
		assertThrows(
				IllegalArgumentException.class,
				() -> shardDatasetAttributes(dimensions, new int[]{0, 64, 64}, new int[]{64, 64, 64}, dataType));
	}

}
