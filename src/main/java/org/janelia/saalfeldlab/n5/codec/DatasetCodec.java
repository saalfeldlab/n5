package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

/**
 * A Codec that transforms the contents of a {@link DataBlock}.
 * <p>
 * This class is N5's analogue to Zarr's array-to-array codec.
 *
 * @param <S> source data type (data contained in decoded blocks)
 * @param <T> target data type (data contained in encoded blocks)
 */
public interface DatasetCodec<S, T> {

	// TODO Name ideas:
	// "ImageCodec"?
	// BlockTransformationCodec

	DataBlock<T> encode(DataBlock<S> block) throws N5IOException;

	DataBlock<S> decode(DataBlock<T> dataBlock) throws N5IOException;

	/**
	 * Create a {@code BlockCodec} that, for encoding, first applies {@code
	 * datasetCodec} and then {@code blockCodec} (and does the same in reverse
	 * order for decoding).
	 *
	 * @return the concatenated BlockCodec
	 */
	static <S, T> BlockCodec<S> concatenate(final DatasetCodec<S, T> datasetCodec, final BlockCodec<T> blockCodec) {

		return new BlockCodec<S>() {

			@Override
			public ReadData encode(final DataBlock<S> dataBlock) throws N5IOException {
				return blockCodec.encode(datasetCodec.encode(dataBlock));
			}

			@Override
			public DataBlock<S> decode(final ReadData readData, final long[] gridPosition) throws N5IOException {
				return datasetCodec.decode(blockCodec.decode(readData, gridPosition));
			}
		};
	}
}
