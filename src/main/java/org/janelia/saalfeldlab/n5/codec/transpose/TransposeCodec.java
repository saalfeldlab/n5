package org.janelia.saalfeldlab.n5.codec.transpose;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.DatasetCodec;

public class TransposeCodec<T> implements DatasetCodec<T> {

	private DataType dataType;
	private final int[] order;

	private final Transpose<T> transpose;

	public TransposeCodec(DataType dataType, int[] order) {

		this.order = order;
		this.dataType = dataType;
		transpose = Transpose.of(dataType, order.length);
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataBlock<?> encode(DataBlock<T> dataBlock) {

		DataBlock<T> encodedBlock = (DataBlock<T>)dataType.createDataBlock(dataBlock.getSize(), dataBlock.getGridPosition(), dataBlock.getNumElements());
		transpose.encode(dataBlock.getData(), encodedBlock.getData(), dataBlock.getSize(), order);
		return encodedBlock;
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataBlock<T> decode(DataBlock<?> dataBlock) {

		DataBlock<T> decodedBlock = (DataBlock<T>)dataType.createDataBlock(dataBlock.getSize(), dataBlock.getGridPosition(), dataBlock.getNumElements());
		transpose.decode((T)dataBlock.getData(), decodedBlock.getData(), dataBlock.getSize(), order);
		return decodedBlock;
	}

	public static boolean isIdentity(int[] permutation) {

		for (int i = 0; i < permutation.length; i++)
			if (permutation[i] != i)
				return false;

		return true;
	}

	public static boolean isReversal(int[] permutation) {

		for (int i = 0; i < permutation.length; i++)
			if (permutation[i] != i)
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
	 * @param first
	 *            the first permutation
	 * @param second
	 *            the second permutation
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
	 * @param p
	 *            the permutation to conjugate
	 * @return the conjugated permutation
	 */
	public static int[] conjugateWithReverse(final int[] p) {

		final int n = p.length;
		final int[] rev = new int[n];
		for (int i = 0; i < n; i++)
			rev[i] = n - i - 1;

		// note that rev is its own inverse
		int[] result = concatenatePermutations(rev, p); // result = rev * p
		result = concatenatePermutations(result, rev); // result = rev * p *
														// rev^-1
		return result;
	}

}
