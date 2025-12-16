package org.janelia.saalfeldlab.n5.codec.transpose;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.codec.DatasetCodec;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * Describes a permutation of the dimensions of a block.
 * <p>
 * The {@code order} parameter parameterizes the permutation.
 * The ith element of the order array gives the destination index of the ith element of the input.
 * Example:
 * 		order  = [1, 2, 0]
 * 		input  = [7, 8, 9] 		// interpret as a block size
 * 		result = [9, 7, 8] 		// permuted block size
 *
 * <p>
 * See the specification of <a href="https://zarr-specs.readthedocs.io/en/latest/v3/codecs/transpose/index.html#transpose-codec">Zarr's Transpose codec<a>.
 */
@NameConfig.Name(value = TransposeCodecInfo.TYPE)
public class TransposeCodecInfo implements DatasetCodecInfo {

	public static final String TYPE = "n5-transpose";

	@NameConfig.Parameter
	private int[] order;

	public TransposeCodecInfo() {
		// for serialization
	}

	public TransposeCodecInfo(int[] order) {

		this.order = order;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public int[] getOrder() {

		return order;
	}

	@Override
	public <T> DatasetCodec<T> create(DatasetAttributes datasetAttributes) {

		validate();
		return new TransposeCodec<T>(datasetAttributes.getDataType(), getOrder());
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof TransposeCodecInfo)
			return Arrays.equals(order, ((TransposeCodecInfo)obj).getOrder());

		return false;
	}

	private void validate() {

		final boolean[] indexFound = new boolean[order.length];
		for( int i : order )
			indexFound[i] = true;

		final int[] missingIndexes = IntStream.range(0, order.length).filter(i -> !indexFound[i]).toArray();
		if( missingIndexes.length > 0 )
			throw new N5Exception("Invalid order for TransposeCodec. Missing indexes: " + Arrays.toString(missingIndexes));
	
	}

	public static TransposeCodecInfo concatenate(TransposeCodecInfo[] infos) {

		if( infos == null || infos.length == 0)
			return null;

		// copy the initial order so we don't modify to the original
		int[] order = new int[infos[0].order.length];
		System.arraycopy(infos[0].order, 0, order, 0, order.length);

		for( int i = 1; i < infos.length; i++ )
			order = TransposeCodec.concatenatePermutations(order, infos[i].order);

		return new TransposeCodecInfo(order);
	}

}