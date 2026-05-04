package org.janelia.saalfeldlab.n5;

public class LongArrayDataBlock extends AbstractDataBlock<long[]> {

	public LongArrayDataBlock(final int[] size, final long[] gridPosition, final long[] data) {

		super(size, gridPosition, data, a -> a.length);
	}
}
