package org.janelia.saalfeldlab.n5.codec;

import java.util.function.UnaryOperator;

/**
 * The size of the blocks a {@link DatasetCodec} receives when encoding.
 * <p>
 * A dataset codec in a chain does not necessarily see blocks of the dataset's
 * chunk size: an earlier codec may have changed the block's shape (see
 * {@link DatasetCodecInfo#encodedBlockSize(int[])}), and chunks at the upper
 * boundary of a dataset may be cropped. Codecs that need to recover the shape
 * of their input when decoding (e.g. a reshape) use this to do so.
 */
public interface SourceBlockSize {

	/**
	 * @return the size of the source block for a full (uncropped) chunk
	 */
	int[] full();

	/**
	 * Returns the size of the source block for the chunk at {@code gridPosition},
	 * cropped to the dataset boundary.
	 *
	 * @param gridPosition
	 *            the position of the chunk on the chunk grid
	 * @return the size of the source block at that position
	 */
	int[] at(long[] gridPosition);

	/**
	 * Creates a {@code SourceBlockSize} for a dataset with the given dimensions
	 * and chunk size, where {@code upstream} maps the size of a dataset-level
	 * chunk to the size of the block passed to the codec on encode (i.e. the
	 * composed {@link DatasetCodecInfo#encodedBlockSize(int[])} of all
	 * preceding codecs).
	 *
	 * @param dimensions
	 *            the dataset dimensions
	 * @param chunkSize
	 *            the (innermost) chunk size
	 * @param upstream
	 *            maps a dataset-level chunk size to the codec's source block size
	 * @return the source block size
	 */
	static SourceBlockSize of(final long[] dimensions, final int[] chunkSize, final UnaryOperator<int[]> upstream) {

		return new SourceBlockSize() {

			@Override
			public int[] full() {

				return upstream.apply(chunkSize.clone());
			}

			@Override
			public int[] at(final long[] gridPosition) {

				final int[] cropped = new int[chunkSize.length];
				for (int d = 0; d < cropped.length; ++d) {
					final long remaining = dimensions[d] - gridPosition[d] * chunkSize[d];
					cropped[d] = (int)Math.max(0, Math.min(chunkSize[d], remaining));
				}
				return upstream.apply(cropped);
			}
		};
	}
}
