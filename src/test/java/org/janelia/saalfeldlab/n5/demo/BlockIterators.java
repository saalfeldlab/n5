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
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.N5BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DeterministicSizeCodecInfo;
import org.janelia.saalfeldlab.n5.util.GridIterator;

public class BlockIterators {

	public static void main(String[] args) {

//		blockIterator();
//		shardBlockIterator();
	}

	public static void shardBlockIterator() {

//		final DatasetAttributes attrs = new DatasetAttributes(
//				new long[] {12, 8},	// image size
//				new int[] {6, 4},		// shard size
//				new int[] {2, 2},		// block size
//				DataType.UINT8,
//				new ShardingCodec(
//						new int[] {2, 2},
//						new CodecInfo[] { new N5BlockCodecInfo() },
//						new DeterministicSizeCodecInfo[] { new RawBlockCodecInfo() },
//						IndexLocation.END
//				));
//
//		shardPositions(attrs)
//			.forEach(x -> System.out.println(Arrays.toString(x)));
	}

//	public static void blockIterator() {
//
//		final DatasetAttributes attrs = new DatasetAttributes(
//				new long[] {12, 8},
//				new int[] {2, 2},
//				DataType.UINT8,
//				new RawCompression());
//
//		blockPositions(attrs).forEach(x -> System.out.println(Arrays.toString(x)));
//	}
//	
//	public static long[] blockGridSize(final DatasetAttributes attrs ) {
//		// this could be a nice method for DatasetAttributes
//
//		return IntStream.range(0, attrs.getNumDimensions()).mapToLong(i -> (long)Math.ceil((double)attrs.getDimensions()[i] / attrs.getBlockSize()[i])).toArray();
//
//	}
//	
//	public static long[] shardGridSize(final DatasetAttributes attrs ) {
//		// this could be a nice method for DatasetAttributes
//
//		return IntStream.range(0, attrs.getNumDimensions()).mapToLong(i -> (long)Math.ceil((double)attrs.getDimensions()[i] / attrs.getShardSize()[i])).toArray();
//
//	}
//
//	public static Stream<long[]> blockPositions( DatasetAttributes attrs ) {
//		return toStream(new GridIterator(blockGridSize(attrs)));
//	}
//
//	public static Stream<long[]> shardPositions( DatasetAttributes attrs ) {
//
//		final int[] blocksPerShard = attrs.getBlocksPerShard();
//		return toStream( new GridIterator(shardGridSize(attrs)))
//				.flatMap( shardPosition -> {
//
//					final int nd = attrs.getNumDimensions();
//					final long[] min = attrs.getBlockPositionFromShardPosition(shardPosition, new int[nd]);
//					return toStream(new GridIterator(GridIterator.int2long(blocksPerShard), min));
//				});
//	}
//
//	public static <T> Stream<T> toStream( final Iterator<T> it ) {
//		return StreamSupport.stream( Spliterators.spliteratorUnknownSize(
//				  it, Spliterator.ORDERED),
//		          false);
//	}

}
