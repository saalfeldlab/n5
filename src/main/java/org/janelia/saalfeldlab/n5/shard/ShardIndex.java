package org.janelia.saalfeldlab.n5.shard;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;

public class ShardIndex extends LongArrayDataBlock {

	private static final int BYTES_PER_LONG = 8;

	public ShardIndex(int[] size, long[] data) {

		super(indexBlockSize(size), new long[]{0}, data);
	}

	public ShardIndex(int[] size) {

		super(indexBlockSize(size), new long[]{0}, createData(size));
	}

	private static long[] createData(final int[] size) {

		final int N = 2 * Arrays.stream(size).reduce(1, (x, y) -> x * y);
		return new long[N];
	}

	private static int[] indexBlockSize(final int[] size) {

		final int[] indexBlockSize = new int[size.length + 1];
		indexBlockSize[0] = 2;
		System.arraycopy(size, 0, indexBlockSize, 1, size.length);
		return indexBlockSize;
	}

	public long getOffset(long... gridPosition) {

		return data[getOffsetIndex(gridPosition)];
	}

	public long getNumBytes(long... gridPosition) {

		return data[getNumBytesIndex(gridPosition)];
	}

	public void set(long offset, long nbytes, long[] gridPosition) {

		final int i = getOffsetIndex(gridPosition);
		data[i] = offset;
		data[i + 1] = nbytes;
	}


	private int getOffsetIndex(long... gridPosition) {

		int idx = 0;
		long stride = 2;
		for (int i = 0; i < gridPosition.length; i++) {
			idx += gridPosition[i] * stride;
			stride *= size[i];
		}

		return idx;
	}

	private int getNumBytesIndex(long... gridPosition) {

		return getOffsetIndex() + 1;
	}

	public void printData() {

		System.out.println(Arrays.toString(data));
	}

	public static ShardIndex read(FileChannel channel, ShardedDatasetAttributes datasetAttributes) throws IOException {

		// TODO need codecs
		// TODO FileChannel is too specific - generalize
		final int[] indexShape = indexBlockSize(datasetAttributes.getShardBlockGridSize());
		final int indexSize = (int)Arrays.stream(indexShape).reduce(1, (x, y) -> x * y);
		final int indexBytes = BYTES_PER_LONG * indexSize;

		if (datasetAttributes.getIndexLocation() == ShardingConfiguration.IndexLocation.END) {
			channel.position(channel.size() - indexBytes);
		}

		final InputStream is = Channels.newInputStream(channel);
		final DataInputStream dis = new DataInputStream(is);

		final long[] indexes = new long[indexSize];
		for (int i = 0; i < indexSize; i++) {
			indexes[i] = dis.readLong();
		}

		return new ShardIndex(indexShape, indexes);
	}

	public static void main(String[] args) {

		final ShardIndex ib = new ShardIndex(new int[]{2, 2});

		ib.set(8, 9, new long[]{1, 1});
		ib.printData();

		// System.out.println(ib.getIndex(0, 0));
		// System.out.println(ib.getIndex(1, 0));
		// System.out.println(ib.getIndex(0, 1));
		// System.out.println(ib.getIndex(1, 1));

		System.out.println("done");
	}

}
