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
 * 		result = [9, 7, 8] 		// permuted chunk size
 *
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
		
		/*
		 * Note:
		 * The implementation layer can not directly consume the permutation stored in 'order'
		 * due to changes of indexing (C- vs F-) order.
		 *
		 * Zarr specifies that a block with size [z, y, x] will be flattened such that the x-dimension
		 * varies fastest and the z-dimension varies slowest. Because it uses F-order, N5 reverses the
		 * order of these dimensions: the same block in N5 will have size [x, y, z].
		 *
		 * If a TranposeCodec is present, conjugating the permutation with a reversal permutation:
		 *  	reverse * order * reverse
		 * gives the proper behavior.
		 *
		 * Example
		 * (C-order)
		 * Suppose order = [1, 2, 0], and labeling the dimensions [z, y, x]
		 * The resulting transposed array will have size [x, z, y].
		 * As a result, the y (x) dimension will vary fastest (slowest).
		 *
		 * (F-order)
		 * The F-order axis relabeling of this is [x, y, z] with x (z) varying fastest (slowest)
		 * The permutation we need to apply has to result in [y, z, x] because
		 * we need the y (x) dimensions to vary fastest (slowest) as above
		 * i.e. the permutation is [2, 0, 1]
		 *
		 * input: 	[x, y, z]
		 * 		"reverse" / "c-to-f-order"
		 *  		[z, y, x]
		 * 		apply the given permutation ([1, 2, 0] in this example)
		 *  		[x, z, y]
		 * 		"un-reverse" / "f-to-c-order"
		 *  		[y, z, x]
		 *
		 * gives the result that we need.
		 */
		validate();
		return new TransposeCodec<T>(datasetAttributes.getDataType(), conjugateWithReverse(getOrder()));
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
			order = concatenatePermutations(order, infos[i].order);

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

	public static int[] invertPermutation(final int[] p) {

		final int[] inv = new int[p.length];
		for (int i = 0; i < p.length; i++)
			inv[p[i]] = i;

		return inv;
	}

	/**
	 * Composes two permutations: result[i] = first[second[i]].
	 * 
	 * @param first the first permutation
	 * @param second the second permutation  
	 * @return the composition of first and second
	 */
	public static int[] concatenatePermutations(final int[] first, final int[] second) {

		int n = first.length;
		final int[] result = new int[n];
		for (int i = 0; i < n; i++) {
			result[i] = first[second[i]];
		}
		return result;
	}

	/**
	 * Conjugates a permutation with the reversal permutation: rev * p * rev^-1,
	 * where rev is the permutation that reverses the elements.
	 * 
	 * @param p the permutation to conjugate
	 * @return the conjugated permutation
	 */
	public static int[] conjugateWithReverse(final int[] p ) {

		final int n = p.length;
		final int[] rev = new int[n];
		for( int i = 0; i < n; i++ )
			rev[i] = n - i - 1;

		// note that rev is its own inverse
		int[] result = concatenatePermutations(rev, p); // result = rev * p
		result = concatenatePermutations(result, rev);  // result = rev * p * rev^-1
		return result;
	}

}