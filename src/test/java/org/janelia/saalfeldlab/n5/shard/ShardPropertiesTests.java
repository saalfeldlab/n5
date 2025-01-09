package org.janelia.saalfeldlab.n5.shard;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Iterator;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.junit.Test;

public class ShardPropertiesTests {

	@Test
	public void testShardProperties() throws Exception {

		final long[] arraySize = new long[]{16, 16};
		final int[] shardSize = new int[]{16, 16};
		final long[] shardPosition = new long[]{1, 1};
		final int[] blkSize = new int[]{4, 4};

		final ShardedDatasetAttributes dsetAttrs = new ShardedDatasetAttributes(
				arraySize,
				shardSize,
				blkSize,
				DataType.UINT8,
				new Codec[]{},
				new DeterministicSizeCodec[]{},
				IndexLocation.END);

		@SuppressWarnings({"rawtypes", "unchecked"})
		final InMemoryShard shard = new InMemoryShard(dsetAttrs, shardPosition, null);

		assertArrayEquals(new int[]{4, 4}, shard.getBlockGridSize());

		assertArrayEquals(new long[]{0, 0}, shard.getShard(0, 0));
		assertArrayEquals(new long[]{1, 1}, shard.getShard(5, 5));
		assertArrayEquals(new long[]{1, 0}, shard.getShard(5, 0));
		assertArrayEquals(new long[]{0, 1}, shard.getShard(0, 5));

//		assertNull(shard.getBlockPosition(0, 0));
//		assertNull(shard.getBlockPosition(3, 3));

		assertArrayEquals(new int[]{0, 0}, shard.getBlockPosition(4, 4));
		assertArrayEquals(new int[]{1, 1}, shard.getBlockPosition(5, 5));
		assertArrayEquals(new int[]{2, 2}, shard.getBlockPosition(6, 6));
		assertArrayEquals(new int[]{3, 3}, shard.getBlockPosition(7, 7));
	}

	@Test
	public void testShardBlockPositionIterator() throws Exception {

		final long[] arraySize = new long[]{16, 16};
		final int[] shardSize = new int[]{16, 16};
		final long[] shardPosition = new long[]{1, 1};
		final int[] blkSize = new int[]{4, 4};

		final ShardedDatasetAttributes dsetAttrs = new ShardedDatasetAttributes(
				arraySize,
				shardSize,
				blkSize,
				DataType.UINT8,
				new Codec[]{},
				new DeterministicSizeCodec[]{},
				IndexLocation.END);

		@SuppressWarnings({"rawtypes", "unchecked"})
		final InMemoryShard shard = new InMemoryShard(dsetAttrs, shardPosition, null);

		int i = 0;
		Iterator<long[]> it = shard.blockPositionIterator();
		long[] p = null;
		while (it.hasNext()) {

			p = it.next();
			if( i == 0 )
				assertArrayEquals(new long[]{4,4}, p);

			i++;
		}
		assertEquals(16,i);
		assertArrayEquals(new long[]{7,7}, p);

	}

}
