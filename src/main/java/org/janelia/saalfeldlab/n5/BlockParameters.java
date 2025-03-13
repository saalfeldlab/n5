package org.janelia.saalfeldlab.n5;

public interface BlockParameters {

	public long[] getDimensions();

	public int getNumDimensions();

	public int[] getBlockSize();

}
