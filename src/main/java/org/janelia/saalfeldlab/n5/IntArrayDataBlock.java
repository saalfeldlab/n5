package org.janelia.saalfeldlab.n5;

public class IntArrayDataBlock extends AbstractDataBlock<int[]> {

	public IntArrayDataBlock(final int[] size, final long[] gridPosition, final int[] data) {

		super(size, gridPosition, data, a -> a.length);
	}
}
