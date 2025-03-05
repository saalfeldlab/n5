package org.janelia.saalfeldlab.n5.codec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataBlock.DataBlockFactory;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.StringDataBlock;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import static org.janelia.saalfeldlab.n5.codec.N5Codecs.BlockHeader.MODE_DEFAULT;
import static org.janelia.saalfeldlab.n5.codec.N5Codecs.BlockHeader.MODE_OBJECT;
import static org.janelia.saalfeldlab.n5.codec.N5Codecs.BlockHeader.MODE_VARLENGTH;

public class N5Codecs {

	public static final DataBlockCodecFactory<byte[]>   BYTE   = c -> new DefaultDataBlockCodec<>(DataCodec.BYTE, ByteArrayDataBlock::new, c);
	public static final DataBlockCodecFactory<short[]>  SHORT  = c -> new DefaultDataBlockCodec<>(DataCodec.SHORT_BIG_ENDIAN, ShortArrayDataBlock::new, c);
	public static final DataBlockCodecFactory<int[]>    INT    = c -> new DefaultDataBlockCodec<>(DataCodec.INT_BIG_ENDIAN, IntArrayDataBlock::new, c);
	public static final DataBlockCodecFactory<long[]>   LONG   = c -> new DefaultDataBlockCodec<>(DataCodec.LONG_BIG_ENDIAN, LongArrayDataBlock::new, c);
	public static final DataBlockCodecFactory<float[]>  FLOAT  = c -> new DefaultDataBlockCodec<>(DataCodec.FLOAT_BIG_ENDIAN, FloatArrayDataBlock::new, c);
	public static final DataBlockCodecFactory<double[]> DOUBLE = c -> new DefaultDataBlockCodec<>(DataCodec.DOUBLE_BIG_ENDIAN, DoubleArrayDataBlock::new, c);
	public static final DataBlockCodecFactory<String[]> STRING = StringDataBlockCodec::new;
	public static final DataBlockCodecFactory<byte[]>   OBJECT = ObjectDataBlockCodec::new;

	private N5Codecs() {}

	public static <T> DataBlockCodec<T> createDataBlockCodec(
			final DataType dataType,
			final Compression compression) {

		final DataBlockCodecFactory<?> factory;
		switch (dataType) {
		case UINT8:
		case INT8:
			factory = N5Codecs.BYTE;
			break;
		case UINT16:
		case INT16:
			factory = N5Codecs.SHORT;
			break;
		case UINT32:
		case INT32:
			factory = N5Codecs.INT;
			break;
		case UINT64:
		case INT64:
			factory = N5Codecs.LONG;
			break;
		case FLOAT32:
			factory = N5Codecs.FLOAT;
			break;
		case FLOAT64:
			factory = N5Codecs.DOUBLE;
			break;
		case STRING:
			factory = N5Codecs.STRING;
			break;
		case OBJECT:
			factory = N5Codecs.OBJECT;
			break;
		default:
			throw new IllegalArgumentException("Unsupported data type: " + dataType);
		}
		final DataBlockCodecFactory<T> tFactory = (DataBlockCodecFactory<T>)factory;
		return tFactory.createDataBlockCodec(compression);
	}

	public interface DataBlockCodecFactory<T> {

		DataBlockCodec<T> createDataBlockCodec(Compression compression);
	}

	/**
	 * DataBlockCodec for all N5 data types, except STRING and OBJECT
	 */
	static class DefaultDataBlockCodec<T> implements DataBlockCodec<T> {

		private final DataCodec<T> dataCodec;

		private final DataBlockFactory<T> dataBlockFactory;

		private final Compression compression;

		DefaultDataBlockCodec(
				final DataCodec<T> dataCodec,
				final DataBlockFactory<T> dataBlockFactory,
				final Compression compression) {
			this.dataCodec = dataCodec;
			this.dataBlockFactory = dataBlockFactory;
			this.compression = compression;
		}

		@Override
		public ReadData encode(final DataBlock<T> dataBlock) throws IOException {
			return ReadData.from(out -> {
				new BlockHeader(dataBlock.getSize(), dataBlock.getNumElements()).writeTo(out);
				compression.encode(dataCodec.serialize(dataBlock.getData())).writeTo(out);
			});
		}

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws IOException {
			try(final InputStream in = readData.inputStream()) {
				final BlockHeader header = BlockHeader.readFrom(in, MODE_DEFAULT, MODE_VARLENGTH);
				final T data = dataCodec.createData(header.numElements());
				final int numBytes = header.numElements() * dataCodec.bytesPerElement();
				final ReadData decompressed = compression.decode(ReadData.from(in), numBytes);
				dataCodec.deserialize(decompressed, data);
				return dataBlockFactory.createDataBlock(header.blockSize(), gridPosition, data);
			}
		}
	}

	/**
	 * DataBlockCodec for N5 data type STRING
	 */
	static class StringDataBlockCodec implements DataBlockCodec<String[]> {

		private static final Charset ENCODING = StandardCharsets.UTF_8;
		private static final String NULLCHAR = "\0";

		private final Compression compression;

		StringDataBlockCodec(final Compression compression) {
			this.compression = compression;
		}

		@Override
		public ReadData encode(final DataBlock<String[]> dataBlock) throws IOException {
			return ReadData.from(out -> {
				final String flattenedArray = String.join(NULLCHAR, dataBlock.getData()) + NULLCHAR;
				final byte[] serializedData = flattenedArray.getBytes(ENCODING);
				new BlockHeader(dataBlock.getSize(), serializedData.length).writeTo(out);
				compression.encode(ReadData.from(serializedData)).writeTo(out);
			});
		}

		@Override
		public DataBlock<String[]> decode(final ReadData readData, final long[] gridPosition) throws IOException {
			try(final InputStream in = readData.inputStream()) {
				final BlockHeader header = BlockHeader.readFrom(in, MODE_DEFAULT, MODE_VARLENGTH);
				final ReadData decompressed = compression.decode(ReadData.from(in), header.numElements());
				final byte[] serializedData = decompressed.allBytes();
				final String rawChars = new String(serializedData, ENCODING);
				final String[] actualData = rawChars.split(NULLCHAR);
				return new StringDataBlock(header.blockSize(), gridPosition, actualData);
			}
		}
	}

	/**
	 * DataBlockCodec for N5 data type OBJECT
	 */
	static class ObjectDataBlockCodec implements DataBlockCodec<byte[]> {

		private final Compression compression;

		ObjectDataBlockCodec(final Compression compression) {
			this.compression = compression;
		}

		@Override
		public ReadData encode(final DataBlock<byte[]> dataBlock) throws IOException {
			return ReadData.from(out -> {
				new BlockHeader(null, dataBlock.getNumElements()).writeTo(out);
				compression.encode(ReadData.from(dataBlock.getData())).writeTo(out);
			});
		}

		@Override
		public DataBlock<byte[]> decode(final ReadData readData, final long[] gridPosition) throws IOException {
			try(final InputStream in = readData.inputStream()) {
				final BlockHeader header = BlockHeader.readFrom(in, MODE_OBJECT);
				final byte[] data = new byte[header.numElements()];
				final ReadData decompressed = compression.decode(ReadData.from(in), data.length);
				new DataInputStream(decompressed.inputStream()).readFully(data);
				return new ByteArrayDataBlock(null, gridPosition, data);
			}
		}
	}

	static class BlockHeader {

		public static final short MODE_DEFAULT = 0;
		public static final short MODE_VARLENGTH = 1;
		public static final short MODE_OBJECT = 2;

		private final short mode;
		private final int[] blockSize;
		private final int numElements;

		BlockHeader(final short mode, final int[] blockSize, final int numElements) {
			this.mode = mode;
			this.blockSize = blockSize;
			this.numElements = numElements;
		}

		BlockHeader(final int[] blockSize, final int numElements) {
			if (blockSize == null) {
				this.mode = MODE_OBJECT;
			} else if (DataBlock.getNumElements(blockSize) == numElements) {
				this.mode = MODE_DEFAULT;
			} else {
				this.mode = MODE_VARLENGTH;
			}
			this.blockSize = blockSize;
			this.numElements = numElements;
		}

		public short mode() {
			return mode;
		}

		public int[] blockSize() {
			return blockSize;
		}

		public int numElements() {
			return numElements;
		}

		private static int[] readBlockSize(final DataInputStream dis) throws IOException {
			final int nDim = dis.readShort();
			final int[] blockSize = new int[nDim];
			for (int d = 0; d < nDim; ++d)
				blockSize[d] = dis.readInt();
			return blockSize;
		}

		private static void writeBlockSize(final int[] blockSize, final DataOutputStream dos) throws IOException {
			dos.writeShort(blockSize.length);
			for (final int size : blockSize)
				dos.writeInt(size);
		}

		void writeTo(final OutputStream out) throws IOException {
			final DataOutputStream dos = new DataOutputStream(out);
			dos.writeShort(mode);
			switch (mode) {
			case MODE_DEFAULT:// default
				writeBlockSize(blockSize, dos);
				break;
			case MODE_VARLENGTH:// varlength
				writeBlockSize(blockSize, dos);
				dos.writeInt(numElements);
				break;
			case MODE_OBJECT: // object
				dos.writeInt(numElements);
				break;
			default:
				throw new IOException("unexpected mode: " + mode);
			}
			dos.flush();
		}

		static BlockHeader readFrom(final InputStream in) throws IOException {
			return readFrom(in, null);
		}

		static BlockHeader readFrom(final InputStream in, short... allowedModes) throws IOException {
			final DataInputStream dis = new DataInputStream(in);
			final short mode = dis.readShort();
			final int[] blockSize;
			final int numElements;
			switch (mode) {
			case MODE_DEFAULT:// default
				blockSize = readBlockSize(dis);
				numElements = DataBlock.getNumElements(blockSize);
				break;
			case MODE_VARLENGTH:// varlength
				blockSize = readBlockSize(dis);
				numElements = dis.readInt();
				break;
			case MODE_OBJECT: // object
				blockSize = null;
				numElements = dis.readInt();
				break;
			default:
				throw new IOException("unexpected mode: " + mode);
			}

			boolean modeIsOk = allowedModes == null || allowedModes.length == 0;
			for (int i = 0; !modeIsOk && i < allowedModes.length; ++i) {
				if (mode == allowedModes[i])
					modeIsOk = true;
			}
			if (!modeIsOk) {
				throw new IOException("unexpected mode: " + mode);
			}

			return new BlockHeader(mode, blockSize, numElements);
		}
	}
}
