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
 * The {@code order} parameter parametrizes the permutation. 
 * The ith element of the order array
 * <p>
 * See the specification of the <a href="https://zarr-specs.readthedocs.io/en/latest/v3/codecs/transpose/index.html#transpose-codec">Zarr's Transpose codec<a>.
 */
@NameConfig.Name(value = TransposeCodecInfo.TYPE)
public class TransposeCodecInfo implements DatasetCodecInfo {

	public static final String TYPE = "transpose";

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
		return new TransposeCodec<T>(datasetAttributes.getDataType(), order);
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof TransposeCodecInfo)
			return Arrays.equals(order, ((TransposeCodecInfo)obj).order);

		return false;
	}

	public static TransposeCodecInfo concatenate(TransposeCodecInfo[] infos) {

		if( infos == null || infos.length == 0)
			return null;

		// copy the initial order so we don't modify to the original
		int[] order = new int[infos[0].order.length];
		System.arraycopy(infos[0].order, 0, order, 0, order.length);

		for( int i = 1; i < infos.length; i++ )
			updatePermutation(order, infos[i].order);

		return new TransposeCodecInfo(order);
	}

	private void validate() {

		final boolean[] indexFound = new boolean[order.length];
		for( int i : order )
			indexFound[i] = true;

		final int[] missingIndexes = IntStream.range(0, order.length).filter(i -> !indexFound[i]).toArray();
		if( missingIndexes.length > 0 )
			throw new N5Exception("Invalid order for TransposeCodec. Missing indexes: " + Arrays.toString(missingIndexes));
	
	}

	/**
	 * Updates the permutation array to be a permutation whose result is
	 * (permutation) followed by (additionalPermutation).
	 *
	 * @param permutation
	 *            the initial permutation to update
	 * @param additionalPermutation
	 *            the additional permutation to concatenate to it
	 */
	private static void updatePermutation(int[] permutation, int[] additionalPermutation) {

		// result_pos[i] = src_pos[order[i]],

		// TODO validate
		int[] temp = new int[permutation.length];
		for (int i = 0; i < permutation.length; i++) {
			temp[i] = permutation[additionalPermutation[i]];
		}

		// Copy the result back to the permutation array
		System.arraycopy(temp, 0, permutation, 0, permutation.length);
	}

	public static boolean isIdentity(TransposeCodecInfo info) {

		for (int i = 0; i < info.order.length; i++)
			if (info.order[i] != i)
				return false;

		return true;
	}

	public static boolean isReversal(TransposeCodecInfo info) {

		for (int i = 0; i < info.order.length; i++)
			if (info.order[i] != i)
				return false;

		return true;
	}
	
	public static void main(String[] args) {
		
		int[] p  = new int[]{2,1,0};
		int[] p2  = new int[]{1,0,2};

		updatePermutation(p, p2);
		System.out.println(Arrays.toString(p));
	}

}