package org.janelia.saalfeldlab.n5;

public class DoubleArrayDataBlock extends AbstractDataBlock<double[]> {

	public DoubleArrayDataBlock(final int[] size, final long[] gridPosition, final double[] data) {

		super(size, gridPosition, data, a -> a.length);
	}
}
