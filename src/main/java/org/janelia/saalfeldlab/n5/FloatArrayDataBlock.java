package org.janelia.saalfeldlab.n5;

public class FloatArrayDataBlock extends AbstractDataBlock<float[]> {

	public FloatArrayDataBlock(final int[] size, final long[] gridPosition, final float[] data) {

		super(size, gridPosition, data, a -> a.length);
	}
}
