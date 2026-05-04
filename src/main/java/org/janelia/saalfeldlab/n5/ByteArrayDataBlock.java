package org.janelia.saalfeldlab.n5;

public class ByteArrayDataBlock extends AbstractDataBlock<byte[]> {

	public ByteArrayDataBlock(final int[] size, final long[] gridPosition, final byte[] data) {

		super(size, gridPosition, data, a -> a.length);
	}
}
