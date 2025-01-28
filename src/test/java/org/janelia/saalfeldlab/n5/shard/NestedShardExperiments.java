package org.janelia.saalfeldlab.n5.shard;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.apache.commons.lang3.stream.IntStreams;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.BytesCodec;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.codec.checksum.Crc32cChecksumCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;

public class NestedShardExperiments {

	final int nd = 2;

	final int[] blockSize = new int[]{3, 2};
	final int[] innerShardSize = new int[] { 3 * blockSize[0], 2 * blockSize[1] };
	final int[] shardSize = new int[] { 9 * blockSize[0], 4 * blockSize[1] };

	final long[] dimensions = new long[] { 27 * blockSize[0], 8 * blockSize[1] };
//	final long[] dimensions = GridIterator.int2long(shardSize);

	public static void main(String[] args) {

//		new NestedShardExperiments().go();
		new NestedShardExperiments().getNextChild();
	}

	public void getNextChild() {

		System.out.println(Arrays.toString(blockSize));
		System.out.println(Arrays.toString(innerShardSize));
		System.out.println(Arrays.toString(shardSize));
		System.out.println(Arrays.toString(dimensions));


		// given a block position, what inner shard position is needed
		final ShardedDatasetAttributes outerInner = shardingParams(dimensions, shardSize, innerShardSize );
		final ShardedDatasetAttributes innerBlock = shardingParams(dimensions, innerShardSize, blockSize );

		long[] blkPos = new long[]{25, 6};
		System.out.println("blk pos: " + Arrays.toString(blkPos));

		long[] innerShardPos = innerBlock.getShardPositionForBlock(blkPos);
		System.out.println("blk pos: " + Arrays.toString(innerShardPos));

		long[] outerShardPos = outerInner.getShardPositionForBlock(innerShardPos);
		System.out.println("blk pos: " + Arrays.toString(outerShardPos));
		

		System.out.println(" ");
		long[] blkMin = IntStream.range(0, nd).mapToLong( i -> {
			return blkPos[i] * blockSize[i];
		}).toArray();
		System.out.println("blk min:  " + Arrays.toString(blkMin));

		long[] shardIdx = IntStream.range(0, nd).mapToLong( i -> {
			return blkMin[i] / shardSize[i];
		}).toArray();
		System.out.println("shard idx:  " + Arrays.toString(shardIdx));
		

	}

	public void go() {
		
		final ShardedDatasetAttributes attrs = getNestedShardCodecsAttributes();
		final long[] pos = new long[] {0,0};

		InMemoryShard outerShard = new InMemoryShard(attrs, pos);

		InMemoryShard<byte[]> innerShard = new InMemoryShard<>(attrs, pos);

		final byte[] data = new byte[ blockSize[0] * blockSize[1]];
		Arrays.fill(data, (byte)4);
		ByteArrayDataBlock blk = new ByteArrayDataBlock(blockSize, pos, data);

		innerShard.addBlock(blk);
		outerShard.addBlock(innerShard);

		System.out.println(outerShard);
		System.out.println(innerShard);
		System.out.println(blk);
		System.out.println("");

		System.out.println(outerShard.getChildBlock(0, 0));
		System.out.println(outerShard.getBlock(0, 0));
		System.out.println("");

		System.out.println(innerShard.getChildBlock(0, 0));
		System.out.println(innerShard.getBlock(0, 0));
		System.out.println("");

		System.out.println(blk.getBlock(0, 0));
	}
	
	private ShardedDatasetAttributes getNestedShardCodecsAttributes() {

		// TODO: its not even clear how we build this given
		// this constructor. Is the block size of the sharded dataset attributes
		// the innermost (block) size or the intermediate shard size?
		// probably better to forget about this class - only use DatasetAttributes
		// and detect shading in another way
		final ShardingCodec innerShard = new ShardingCodec(innerShardSize,
				new Codec[] { new BytesCodec() },
				new DeterministicSizeCodec[] { new BytesCodec(), new Crc32cChecksumCodec() },
				IndexLocation.START);

		return new ShardedDatasetAttributes(dimensions, shardSize, blockSize, DataType.UINT8,
				new Codec[] { innerShard },
				new DeterministicSizeCodec[] { new BytesCodec(), new Crc32cChecksumCodec() },
				IndexLocation.END);
	}
	
	private static ShardedDatasetAttributes shardingParams(long[] dimensions, int[] shardSize, int[] blockSize) {

		return new ShardedDatasetAttributes(dimensions, shardSize, blockSize, DataType.UINT8,
				new Codec[] { new BytesCodec() },
				new DeterministicSizeCodec[] { new BytesCodec(), new Crc32cChecksumCodec() }, IndexLocation.END);
	}

}
