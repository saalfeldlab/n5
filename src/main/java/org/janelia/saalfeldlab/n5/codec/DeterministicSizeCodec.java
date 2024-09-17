package org.janelia.saalfeldlab.n5.codec;

/**
 * A {@link Codec} that can deterministically determine the size of encoded data from the size of the raw data and vice versa from the data length alone (i.e. encoding is data
 * independent).
 */
public interface DeterministicSizeCodec extends Codec {

	public abstract long encodedSize(long size);

	public abstract long decodedSize(long size);

}
