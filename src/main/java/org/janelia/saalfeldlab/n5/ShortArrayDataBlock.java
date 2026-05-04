package org.janelia.saalfeldlab.n5;

public class ShortArrayDataBlock extends AbstractDataBlock<short[]> {

	public ShortArrayDataBlock(final int[] size, final long[] gridPosition, final short[] data) {

		super(size, gridPosition, data, a -> a.length);
	}
}
