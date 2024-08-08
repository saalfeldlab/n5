package org.janelia.saalfeldlab.n5;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;

import org.janelia.saalfeldlab.n5.codec.ByteStreamCodec;
import org.janelia.saalfeldlab.n5.codec.ComposedCodec;

/**
 * Mandatory dataset attributes:
 *
 * <ol>
 * <li>long[] : dimensions</li>
 * <li>int[] : blockSize</li>
 * <li>{@link DataType} : dataType</li>
 * <li>{@link Compression} : compression</li>
 * </ol>
 *
 * Optional dataset attributes:
 * <ol>
 * <li>{@link ByteStreamCodec}[] : codecs</li>
 * </ol>
 *
 * @author Stephan Saalfeld
 *
 */
public class DatasetAttributes implements Serializable {

	private static final long serialVersionUID = -4521467080388947553L;

	public static final String DIMENSIONS_KEY = "dimensions";
	public static final String BLOCK_SIZE_KEY = "blockSize";
	public static final String DATA_TYPE_KEY = "dataType";
	public static final String COMPRESSION_KEY = "compression";
	public static final String CODEC_KEY = "codecs";

	public static final String[] N5_DATASET_ATTRIBUTES = new String[]{
			DIMENSIONS_KEY, BLOCK_SIZE_KEY, DATA_TYPE_KEY, COMPRESSION_KEY, CODEC_KEY
	};

	/* version 0 */
	protected static final String compressionTypeKey = "compressionType";

	private final long[] dimensions;
	private final int[] blockSize;
	private final DataType dataType;
	private final Compression compression;
	private final Codec[] codecs;

	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression,
			final Codec[] codecs) {

		this.dimensions = dimensions;
		this.blockSize = blockSize;
		this.dataType = dataType;
		this.codecs = codecs;
		this.compression = compression;
	}

	public DatasetAttributes(
			final long[] dimensions,
			final int[] blockSize,
			final DataType dataType,
			final Compression compression) {

		this(dimensions, blockSize, dataType, compression, null);
	}

	public long[] getDimensions() {

		return dimensions;
	}

	public int getNumDimensions() {

		return dimensions.length;
	}

	public int[] getBlockSize() {

		return blockSize;
	}

	public Compression getCompression() {

		return compression;
	}

	public DataType getDataType() {

		return dataType;
	}

	public Codec[] getCodecs() {

		return codecs;
	}

	/**
	 * Assumes a valid collection of codecs.
	 *
	 * @return the ByteStreamCodec
	 */
	public ByteStreamCodec collectCodecs() {

		final ByteStreamCodec compressionCodec = Compression.getCompressionAsCodec(compression);

		// if there is exactly one codec, then it must be an array->bytes
		if (codecs == null || codecs.length <= 1)
			return compressionCodec;
		else {

			final ByteStreamCodec[] codecsAndCompresor = new ByteStreamCodec[codecs.length + 1];
			final ByteStreamCodec[] byteCodecs = Codec.extractByteCodecs(getCodecs());
			for (int i = 0; i < byteCodecs.length; i++)
				codecsAndCompresor[i] = byteCodecs[i];

			codecsAndCompresor[codecs.length] = compressionCodec;
			return new ComposedCodec(codecsAndCompresor);
		}
	}

	public HashMap<String, Object> asMap() {

		final HashMap<String, Object> map = new HashMap<>();
		map.put(DIMENSIONS_KEY, dimensions);
		map.put(BLOCK_SIZE_KEY, blockSize);
		map.put(DATA_TYPE_KEY, dataType);
		map.put(COMPRESSION_KEY, compression);
		map.put(CODEC_KEY, codecs); // TODO : consider not adding to map when null
		return map;
	}

	static DatasetAttributes from(
			final long[] dimensions,
			final DataType dataType,
			int[] blockSize,
			Compression compression,
			final String compressionVersion0Name) {

		return from(dimensions, dataType, blockSize, compression, compressionVersion0Name, null);
	}

	static DatasetAttributes from(
			final long[] dimensions,
			final DataType dataType,
			int[] blockSize,
			Compression compression,
			final String compressionVersion0Name,
			ByteStreamCodec[] codecs) {

		if (blockSize == null)
			blockSize = Arrays.stream(dimensions).mapToInt(a -> (int)a).toArray();

		/* version 0 */
		if (compression == null) {
			switch (compressionVersion0Name) {
			case "raw":
				compression = new RawCompression();
				break;
			case "gzip":
				compression = new GzipCompression();
				break;
			case "bzip2":
				compression = new Bzip2Compression();
				break;
			case "lz4":
				compression = new Lz4Compression();
				break;
			case "xz":
				compression = new XzCompression();
				break;
			}
		}

		return new DatasetAttributes(dimensions, blockSize, dataType, compression, codecs);
	}
}
