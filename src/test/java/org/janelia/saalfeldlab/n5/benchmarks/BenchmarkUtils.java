package org.janelia.saalfeldlab.n5.benchmarks;

import java.util.Random;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.Lz4Compression;
import org.janelia.saalfeldlab.n5.RawCompression;
import org.janelia.saalfeldlab.n5.XzCompression;

public class BenchmarkUtils {

	public static final String GZIP_COMPRESSION = "gzip";
	public static final String RAW_COMPRESSION = "raw";
	public static final String LZ4_COMPRESSION = "lz4";
	public static final String XZ_COMPRESSION = "xz";

	static final Random random = new Random(7777);
	
	public static void fillBlocks(DataBlock<?>[] blocks, DataType dtype, int[] blockSize, long[] p) {

		for (int i = 0; i < blocks.length; i++) {
			final DataBlock<?> blk = dtype.createDataBlock(blockSize, p.clone());
			BenchmarkUtils.fillBlock(dtype, blk);
			blocks[i] = blk;
			p[0]++;
		}
	}

	public static void fillBlock(DataType dtype, DataBlock<?> blk) {

		switch (dtype) {
		case INT32:
			fill((int[])blk.getData());
			break;
		case FLOAT32:
			fill((float[])blk.getData());
			break;
		case FLOAT64:
			fill((double[])blk.getData());
			break;
		case INT16:
			fill((short[])blk.getData());
			break;
		case INT64:
			fill((long[])blk.getData());
			break;
		case INT8:
			fill((byte[])blk.getData());
			break;
		case UINT16:
			fill((short[])blk.getData());
			break;
		case UINT32:
			fill((int[])blk.getData());
			break;
		case UINT64:
			fill((long[])blk.getData());
			break;
		case UINT8:
			fill((byte[])blk.getData());
			break;
		default:
			break;
		}
	}

	public static void fill(short[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = (short)random.nextInt();
	}

	public static void fill(int[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = random.nextInt();
	}

	public static void fill(long[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = random.nextLong();
	}

	public static void fill(float[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = random.nextFloat();
	}

	public static void fill(double[] arr) {
		for (int i = 0; i < arr.length; i++)
			arr[i] = random.nextDouble();
	}

	public static void fill(byte[] arr) {
		random.nextBytes(arr);
	}

	public static Compression getCompression(final String compressionArg) {

		switch (compressionArg) {
		case GZIP_COMPRESSION:
			return new GzipCompression();
		case LZ4_COMPRESSION:
			return new Lz4Compression();
		case XZ_COMPRESSION:
			return new XzCompression();
		case RAW_COMPRESSION:
			return new RawCompression();
		default:
			throw new IllegalArgumentException("No compressor matches: " + compressionArg);
		}
	}
}
