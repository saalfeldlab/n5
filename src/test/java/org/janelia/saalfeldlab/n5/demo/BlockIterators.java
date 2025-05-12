package org.janelia.saalfeldlab.n5.demo;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodec;
import org.janelia.saalfeldlab.n5.codec.RawBytes;
import org.janelia.saalfeldlab.n5.codec.Codec;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodec;
import org.janelia.saalfeldlab.n5.shard.ShardingCodec.IndexLocation;
import org.janelia.saalfeldlab.n5.util.GridIterator;

public class BlockIterators {

	public static void main(String[] args) {

//		blockIterator();
		shardBlockIterator();
	}

	public static void shardBlockIterator() {

		final ShardedDatasetAttributes attrs = new ShardedDatasetAttributes(
				new long[] {12, 8},	// image size
				new int[] {6, 4},		// shard size
				new int[] {2, 2},		// block size
				DataType.UINT8,
				new Codec[] { new N5BlockCodec<>() },
				new DeterministicSizeCodec[] { new N5BlockCodec<>() },
				IndexLocation.END);

		shardPositions(attrs)
			.forEach(x -> System.out.println(Arrays.toString(x)));
	}

	public static void blockIterator() {

		final DatasetAttributes attrs = new DatasetAttributes(
				new long[] {12, 8},
				new int[] {2, 2},
				DataType.UINT8,
				new RawCompression());

		blockPositions(attrs).forEach(x -> System.out.println(Arrays.toString(x)));
	}
	
	public static long[] blockGridSize(final DatasetAttributes attrs ) {
		// this could be a nice method for DatasetAttributes

		return IntStream.range(0, attrs.getNumDimensions()).mapToLong(i -> {
			return (long)Math.ceil(attrs.getDimensions()[i] / attrs.getBlockSize()[i]);
		}).toArray();

	}
	
	public static long[] shardGridSize(final ShardedDatasetAttributes attrs ) {
		// this could be a nice method for DatasetAttributes

		return IntStream.range(0, attrs.getNumDimensions()).mapToLong(i -> {
			return (long)Math.ceil(attrs.getDimensions()[i] / attrs.getShardSize()[i]);
		}).toArray();

	}

	public static Stream<long[]> blockPositions( DatasetAttributes attrs ) {
		return toStream(new GridIterator(blockGridSize(attrs)));
	}

	public static Stream<long[]> shardPositions( ShardedDatasetAttributes attrs ) {

		final int[] blocksPerShard = attrs.getBlocksPerShard();
		return toStream( new GridIterator(shardGridSize(attrs)))
				.flatMap( shardPosition -> {

					final int nd = attrs.getNumDimensions();
					final long[] min = attrs.getBlockPositionFromShardPosition(shardPosition, new long[nd]);
					return toStream(new GridIterator(GridIterator.int2long(blocksPerShard), min));
				});
	}

	public static <T> Stream<T> toStream( final Iterator<T> it ) {
		return StreamSupport.stream( Spliterators.spliteratorUnknownSize(
				  it, Spliterator.ORDERED),
		          false);
	}

}
