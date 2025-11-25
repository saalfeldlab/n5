package org.janelia.saalfeldlab.n5.codec.transpose;

import java.util.Arrays;
import org.janelia.saalfeldlab.n5.DataType;

/**
 * Exploration for implementing "transpose" codec in N5
 */
public class TransposePlayground {

	public static void main(String[] args) {
		transpose2D();
		System.out.println("\n\n --------------------------------------- \n\n");
		transpose3D();
	}

	public static void transpose2D() {

		Transpose<int[]> transpose = Transpose.of(DataType.UINT32, 2);

		int[] values = {
				1, 2, 3,
				4, 5, 6
		};

		int[] decoded_size = {3, 2};
		int[] order = {1, 0};
		System.out.println("decoded_size = " + Arrays.toString(decoded_size));
		System.out.println("order = " + Arrays.toString(order));

		System.out.println("values = \n" + toString(values, decoded_size));

		int[] encoded = new int[values.length];
		transpose.encode(values, encoded, decoded_size, order);

		int[] encoded_size = Transpose.encode(decoded_size, order);
		System.out.println("encoded = \n" + toString(encoded, encoded_size));

		int[] decoded = new int[values.length];
		transpose.decode(encoded, decoded, decoded_size, order);

		System.out.println("decoded = \n" + toString(decoded, decoded_size));
	}

	public static void transpose3D() {

//		Transpose<int[]> transpose = new Transpose<>(Transpose.MemCopy.INT, 3);
		Transpose<int[]> transpose = Transpose.of(DataType.UINT32, 3);

		int[] decoded_size = {4, 3, 2};
		int[] order = {1, 2, 0};
		System.out.println("decoded_size = " + Arrays.toString(decoded_size));
		System.out.println("order = " + Arrays.toString(order));

		int[] encoded_size = Transpose.encode(decoded_size, order);
		System.out.println("encoded_size = " + Arrays.toString(encoded_size));

		int[] values = new int[4 * 3 * 2];
		Arrays.setAll(values, i -> i);
		System.out.println("values = \n" + toString(values, decoded_size));

		int[] encoded = new int[values.length];
		transpose.encode(values, encoded, decoded_size, order);
		System.out.println("encoded = \n" + toString(encoded, encoded_size));

		int[] decoded = new int[values.length];
		transpose.decode(encoded, decoded, decoded_size, order);
		System.out.println("decoded = \n" + toString(decoded, decoded_size));
	}

	static String toString(int[] values, int[] size) {
		StringBuilder str = new StringBuilder();
		final int w = size[0];
		final int h = size[1];
		for (int i = 0; i < values.length; i += w) {
			str.append(Arrays.toString(Arrays.copyOfRange(values, i, Math.min(values.length, i + w)))).append("\n");
			if ((i + w) % (w * h) == 0)
				str.append("\n");
		}
		return str.toString();
	}
}
