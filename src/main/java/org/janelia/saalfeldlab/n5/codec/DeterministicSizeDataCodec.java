package org.janelia.saalfeldlab.n5.codec;

/**
 * A {@link DataCodec} that can deterministically determine the size of encoded
 * data from the size of the raw data (i.e. encoding is data independent).
 */
public interface DeterministicSizeDataCodec extends DataCodec {

	/**
	 * Given {@code size} bytes of raw data, how many bytes will the encoded
	 * data have.
	 *
	 * @param size in bytes
	 * @return encoded size in bytes
	 */
	long encodedSize(long size);
}
