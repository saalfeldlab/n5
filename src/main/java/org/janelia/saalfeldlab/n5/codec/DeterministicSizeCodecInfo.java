package org.janelia.saalfeldlab.n5.codec;

/**
 * A {@link CodecInfo} that can deterministically determine the size of encoded
 * data from the size of the raw data and vice versa from the data length alone
 * (i.e. encoding is data independent).
 */
public interface DeterministicSizeCodecInfo extends CodecInfo {

	long encodedSize(long size);

	long decodedSize(long size);

}
