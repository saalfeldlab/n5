package org.janelia.saalfeldlab.n5.codec;

/**
 * A {@link Codec} that can deterministically determine the size of encoded data
 * from the size of the raw data and vice versa from the data length alone (i.e.
 * encoding is data independent).
 */
public interface DeterministicSizeCodec extends Codec {

	// TODO: Add javadoc. In particular: what is the unit of size? bytes?
	long encodedSize(long size);

	// TODO: Add javadoc. In particular: what is the unit of size? bytes?
	long decodedSize(long size);

}
