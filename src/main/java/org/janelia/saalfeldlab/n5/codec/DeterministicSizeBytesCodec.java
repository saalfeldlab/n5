package org.janelia.saalfeldlab.n5.codec;

/**
 * A {@link BytesCodec} that can deterministically determine the size of encoded
 * data from the size of the raw data (i.e. encoding is data independent).
 */
public interface DeterministicSizeBytesCodec extends BytesCodec {

	/**
	 * Given {@code size} bytes of raw data, how many bytes will the encoded
	 * data have.
	 *
	 * @param size in bytes
	 * @return encoded size in bytes
	 */
	long encodedSize(long size);

	/**
	 * Create a {@code DeterministicSizeBytesCodec} that sequentially applies
	 * {@code codecs} in the given order for encoding, and in reverse order for
	 * decoding.
	 *
	 * @param codecs
	 *            a list of DeterministicSizeBytesCodec
	 * @return the concatenated DeterministicSizeBytesCodec
	 */
	static DeterministicSizeBytesCodec concatenate(final DeterministicSizeBytesCodec... codecs) {

		if (codecs == null)
			throw new NullPointerException();

		if (codecs.length == 1)
			return codecs[0];

		return new ConcatenatedDeterministicSizeBytesCodec(codecs);
	}
}
