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
package org.janelia.saalfeldlab.n5;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.codec.RawBytes;
import org.janelia.saalfeldlab.n5.shard.InMemoryShard;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;
import org.janelia.saalfeldlab.n5.util.Position;
import org.junit.Test;

/**
 * Unit tests for DatasetAttributes class.
 */
public class DatasetAttributesTest {

	/**
	 * Test that validateBlockShardSizes method accepts valid shard and block size combinations.
	 */
	@Test
	public void testValidateBlockShardSizesValid() {

		// Test case 1: shard size equals block size
		long[] dimensions = new long[]{100, 200, 300};
		int[] shardSize = new int[]{64, 64, 64};
		int[] blockSize = new int[]{64, 64, 64};
		DataType dataType = DataType.UINT8;

		// This should not throw any exception
		DatasetAttributes attrs = new DatasetAttributes(dimensions, shardSize, blockSize, dataType);
		assertEquals(shardSize, attrs.getShardSize());
		assertEquals(blockSize, attrs.getBlockSize());
		assertArrayEquals(new int[]{1, 1, 1}, attrs.getBlocksPerShard());

		// Test case 2: shard size is a multiple of block size
		shardSize = new int[]{128};
		blockSize = new int[]{64};
		attrs = new DatasetAttributes(new long[]{128}, shardSize, blockSize, dataType);
		assertEquals(shardSize, attrs.getShardSize());
		assertEquals(blockSize, attrs.getBlockSize());
		assertArrayEquals(new int[]{2}, attrs.getBlocksPerShard());

		// Test case 3: different multiples per dimension
		shardSize = new int[]{128, 256, 32, 2};
		blockSize = new int[]{32, 64, 32, 1};
		attrs = new DatasetAttributes(new long[]{128, 128, 128, 128}, shardSize, blockSize, dataType);
		assertEquals(shardSize, attrs.getShardSize());
		assertEquals(blockSize, attrs.getBlockSize());
		assertArrayEquals(new int[]{4, 4, 1, 2}, attrs.getBlocksPerShard());

		// Test case 4: large multiples
		shardSize = new int[]{1024, 2048, 512};
		blockSize = new int[]{32, 64, 16};
		attrs = new DatasetAttributes(dimensions, shardSize, blockSize, dataType);
		assertEquals(shardSize, attrs.getShardSize());
		assertEquals(blockSize, attrs.getBlockSize());
		assertArrayEquals(new int[]{32, 32, 32}, attrs.getBlocksPerShard());
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
				() -> new DatasetAttributes(dimensions, new int[]{1, 1, 1}, new int[]{1, 0, -1}, dataType));
		assertTrue(ex0.getMessage().contains("<= 0"));

		// Different number of dimensions
		IllegalArgumentException ex1 = assertThrows(
				IllegalArgumentException.class,
				() -> new DatasetAttributes(dimensions, new int[]{64, 64}, new int[]{32, 32, 32}, dataType));
		assertTrue(ex1.getMessage().contains("must equal number of dimensions"));

		// Shard size smaller than block size
		IllegalArgumentException ex2 = assertThrows(
				IllegalArgumentException.class,
				() -> new DatasetAttributes(dimensions, new int[]{32, 64, 64}, new int[]{64, 64, 64}, dataType));
		assertTrue(ex2.getMessage().contains("is larger than the block size"));

		// Shard size not a multiple of block size
		IllegalArgumentException ex3 = assertThrows(
				IllegalArgumentException.class,
				() -> new DatasetAttributes(dimensions, new int[]{100, 100, 100}, new int[]{64, 64, 64}, dataType));
		assertTrue(ex3.getMessage().contains("not a multiple of the block size"));

		// Multiple violations - shard smaller than block in one dimension
		IllegalArgumentException ex4 = assertThrows(
				IllegalArgumentException.class,
				() -> new DatasetAttributes(dimensions, new int[]{128, 32, 128}, new int[]{64, 64, 64}, dataType));
		assertTrue(ex4.getMessage().contains("is larger than the block size"));

		// Edge case - shard size of 0
		assertThrows(
				IllegalArgumentException.class,
				() -> new DatasetAttributes(dimensions, new int[]{0, 64, 64}, new int[]{64, 64, 64}, dataType));
	}

	@Test
	public void testShardProperties() {

		final long[] arraySize = new long[]{16, 16};
		final int[] shardSize = new int[]{16, 16};
		final long[] shardPosition = new long[]{1, 1};
		final int[] blkSize = new int[]{4, 4};

		final DatasetAttributes dsetAttrs = new DatasetAttributes(
				arraySize,
				shardSize,
				blkSize,
				DataType.UINT8,
				new ShardingCodec(
						blkSize,
						new Codec[]{new N5BlockCodec()},
						new DeterministicSizeCodec[]{new RawBytes()},
						IndexLocation.END
				)
		);

		final InMemoryShard shard = new InMemoryShard(dsetAttrs, shardPosition, null);

		assertArrayEquals(new int[]{4, 4}, shard.getBlockGridSize());

		assertArrayEquals(new long[]{0, 0}, dsetAttrs.getShardPositionForBlock(0, 0));
		assertArrayEquals(new long[]{1, 1}, dsetAttrs.getShardPositionForBlock(5, 5));
		assertArrayEquals(new long[]{1, 0}, dsetAttrs.getShardPositionForBlock(5, 0));
		assertArrayEquals(new long[]{0, 1}, dsetAttrs.getShardPositionForBlock(0, 5));

		assertArrayEquals(new int[]{0, 0}, shard.getRelativeBlockPosition(4, 4));
		assertArrayEquals(new int[]{1, 1}, shard.getRelativeBlockPosition(5, 5));
		assertArrayEquals(new int[]{2, 2}, shard.getRelativeBlockPosition(6, 6));
		assertArrayEquals(new int[]{3, 3}, shard.getRelativeBlockPosition(7, 7));
	}

	@Test
	public void testShardGrouping() {

		final long[] arraySize = new long[]{8, 12};
		final int[] shardSize = new int[]{4, 6};
		final int[] blkSize = new int[]{2, 3};

		final DatasetAttributes attrs = new DatasetAttributes(
				arraySize,
				shardSize,
				blkSize,
				DataType.UINT8,
				new ShardingCodec(
						blkSize,
						new Codec[]{ new N5BlockCodec() },
						new DeterministicSizeCodec[]{new RawBytes()},
						IndexLocation.END
				)
		);

		List<long[]> blockPositions = blockPositions(attrs).collect(Collectors.toList());
		final Map<Position, List<long[]>> result = attrs.groupBlockPositions(blockPositions);

		// there are four shards in this image
		assertEquals(4, result.size());

		// there are four blocks per shard in this image
		result.values().forEach(x -> assertEquals(4, x.size()));
	}

	private static Stream<long[]> blockPositions( final DatasetAttributes attrs  ) {

		final int nd = attrs.getNumDimensions();
		final int[] blocksPerShard = attrs.getBlocksPerShard();
		return toStream( new GridIterator(attrs.shardsPerDataset()))
				.flatMap( shardPosition -> {
					final long[] min = attrs.getBlockPositionFromShardPosition(shardPosition, new int[nd]);
					return toStream(new GridIterator(GridIterator.int2long(blocksPerShard), min));
				});
	}

	private static <T> Stream<T> toStream( final Iterator<T> it ) {
		return StreamSupport.stream( Spliterators.spliteratorUnknownSize(
				  it, Spliterator.ORDERED),
		          false);
	}

}