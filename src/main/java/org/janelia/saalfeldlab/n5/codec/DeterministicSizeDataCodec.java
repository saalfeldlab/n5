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

	/**
	 * Create a {@code DeterministicSizeDataCodec} that sequentially applies
	 * {@code codecs} in the given order for encoding, and in reverse order for
	 * decoding.
	 *
	 * @param codecs
	 *            a list of DeterministicSizeDataCodec
	 * @return the concatenated DeterministicSizeDataCodec
	 */
	static DeterministicSizeDataCodec concatenate(final DeterministicSizeDataCodec... codecs) {

		if (codecs == null)
			throw new NullPointerException();

		if (codecs.length == 1)
			return codecs[0];

		return new ConcatenatedDeterministicSizeDataCodec(codecs);
	}

}
