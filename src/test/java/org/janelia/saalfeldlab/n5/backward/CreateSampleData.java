package org.janelia.saalfeldlab.n5.backward;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.RawCompression;

public class CreateSampleData {

	public static void main(String[] args) throws IOException {

		File f = new File("src/test/resources/data-4.0.0-alpha-X.n5");
		System.out.println(f.getCanonicalPath());
		createSampleData(f.getCanonicalPath(), "raw", new RawCompression());
	}

	public static N5FSWriter createSampleData(String baseDir, String dataset, Compression compression) throws IOException {

		N5FSWriter n5 = new N5FSWriter(baseDir);
		final String dsetPath = compression.getType();

		long[] dimensions = new long[]{7, 5};
		int[] blkSizeDset = new int[]{5, 4};
		int[] blkSize = new int[]{5, 4};

		final DatasetAttributes attrs = new DatasetAttributes(dimensions, blkSizeDset, DataType.UINT8, compression);
		n5.createDataset(dsetPath, attrs);

		byte val = 0;
		long[] pos = new long[]{0, 0};
		n5.writeBlock(dsetPath, attrs, createDataBlock(blkSize, pos, val));

		pos[0] = 1;
		pos[1] = 0;
		blkSize[0] = 2;
		blkSize[1] = 4;
		val++;
		n5.writeBlock(dsetPath, attrs, createDataBlock(blkSize, pos, val));

		pos[0] = 0;
		pos[1] = 1;
		blkSize[0] = 5;
		blkSize[1] = 1;
		val++;
		n5.writeBlock(dsetPath, attrs, createDataBlock(blkSize, pos, val));

		pos[0] = 1;
		pos[1] = 1;
		blkSize[0] = 2;
		blkSize[1] = 1;
		val++;
		n5.writeBlock(dsetPath, attrs, createDataBlock( blkSize, pos, val ));

		return n5;
	}

	public static ByteArrayDataBlock createDataBlock(int[] size, long[] gridPosition, byte value) throws IOException {
		int N = Arrays.stream(size).reduce(1, (x,y) -> x*y);
		final byte[] data = new byte[N];
		Arrays.fill(data, value);
		return new ByteArrayDataBlock(size, gridPosition, data);
	}
}
