package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * De/serialize {@link DataBlock} from/to {@link ReadData}.
 *
 * @param <T>
 * 		type of the data contained in the DataBlock
 */
public interface BlockCodec<T> {

	/**
	 * Serializes a {@link DataBlock} into a {@link ReadData} representation for
	 * storage.
	 * <p>
	 * The encoding process serializes the block's data, applies any configured
	 * compression, and may include metadata depending on the codec
	 * implementation.
	 *
	 * @param dataBlock
	 *            the data block to encode
	 *
	 * @return serialized representation of the data block
	 *
	 * @throws N5IOException
	 *             if encoding or compression fails
	 *
	 * @see #decode(ReadData, long[])
	 */
	ReadData encode(DataBlock<T> dataBlock) throws N5IOException;

	/**
	 * Deserializes a {@link DataBlock} from its {@link ReadData}
	 * representation.
	 * <p>
	 * Reverses the encoding process by decompressing (if needed) and
	 * deserializing the data.
	 *
	 * @param readData
	 *            the serialized data to decode
	 * @param gridPosition
	 *            position of this block on the block grid (level 0 coordinates)
	 *
	 * @return reconstructed data block with deserialized data and grid position
	 *
	 * @throws N5IOException
	 *             if decoding, decompression, or data validation fails
	 *
	 * @see #encode(DataBlock)
	 */
	DataBlock<T> decode(ReadData readData, long[] gridPosition) throws N5IOException;

	/**
	 * Given the {@code blockSize} of a {@code DataBlock<T>} return the size of
	 * the encoded block in bytes.
	 * <p>
	 * A {@code UnsupportedOperationException} is thrown, if this {@code
	 * BlockCodec} cannot determine encoded size independent of block content.
	 * For example, if the block type contains var-length elements or if the
	 * serializer uses a non-deterministic {@code DataCodec}.
	 *
	 * @param blockSize
	 * 		size of the block to be encoded
	 *
	 * @return size of the encoded block in bytes
	 *
	 * @throws UnsupportedOperationException
	 * 		if this {@code DataBlockSerializer} cannot determine encoded size independent of block content
	 */
	default long encodedSize(int[] blockSize) throws UnsupportedOperationException {

		throw new UnsupportedOperationException();
	}
}
