package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataBlock.DataBlockFactory;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.StringDataBlock;
import org.janelia.saalfeldlab.n5.codec.Codec.BytesCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.janelia.saalfeldlab.n5.codec.N5Codecs.BlockHeader.MODE_DEFAULT;
import static org.janelia.saalfeldlab.n5.codec.N5Codecs.BlockHeader.MODE_OBJECT;
import static org.janelia.saalfeldlab.n5.codec.N5Codecs.BlockHeader.MODE_VARLENGTH;

public class N5Codecs {

	public static final N5DataBlockCodecFactory<byte[]> BYTE = (codecs) -> new DefaultDataBlockCodec<>(DataCodec.BYTE, ByteArrayDataBlock::new, codecs);
	public static final N5DataBlockCodecFactory<short[]> SHORT = (codecs) -> new DefaultDataBlockCodec<>(DataCodec.SHORT_BIG_ENDIAN, ShortArrayDataBlock::new, codecs);
	public static final N5DataBlockCodecFactory<int[]> INT = (codecs) -> new DefaultDataBlockCodec<>(DataCodec.INT_BIG_ENDIAN, IntArrayDataBlock::new, codecs);
	public static final N5DataBlockCodecFactory<long[]> LONG = (codecs) -> new DefaultDataBlockCodec<>(DataCodec.LONG_BIG_ENDIAN, LongArrayDataBlock::new, codecs);
	public static final N5DataBlockCodecFactory<float[]> FLOAT = (codecs) -> new DefaultDataBlockCodec<>(DataCodec.FLOAT_BIG_ENDIAN, FloatArrayDataBlock::new, codecs);
	public static final N5DataBlockCodecFactory<double[]> DOUBLE = (codecs) -> new DefaultDataBlockCodec<>(DataCodec.DOUBLE_BIG_ENDIAN, DoubleArrayDataBlock::new, codecs);
	public static final N5DataBlockCodecFactory<String[]> STRING = (codecs) -> new StringDataBlockCodec(DataCodec.STRING, StringDataBlock::new, codecs);
	public static final N5DataBlockCodecFactory<byte[]> OBJECT = (codecs) -> new ObjectDataBlockCodec(DataCodec.OBJECT, ByteArrayDataBlock::new, codecs);

	private N5Codecs() {
	}

	public static <T> DataBlockCodec<T> createDataBlockCodec(
			final DataType dataType,
			@Nullable final BytesCodec... codecs) {

		final N5DataBlockCodecFactory<?> factory;
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
		final N5DataBlockCodecFactory<T> tFactory = (N5DataBlockCodecFactory<T>)factory;
		return tFactory.createDataBlockCodec(codecs);
	}

	public interface N5DataBlockCodecFactory<T> {

		DataBlockCodec<T> createDataBlockCodec(@Nullable BytesCodec... codecs);
	}

	public abstract static class AbstractDataBlockCodec<T> implements DataBlockCodec<T> {

		private final DataCodec<T> dataCodec;
		private final DataBlockFactory<T> dataBlockFactory;
		private final BytesCodec[] codecs;

		public AbstractDataBlockCodec(
				final DataCodec<T> dataCodec,
				final DataBlockFactory<T> dataBlockFactory,
				final BytesCodec... codecs
		) {

			this.dataCodec = dataCodec;
			this.dataBlockFactory = dataBlockFactory;
			this.codecs = codecs;
		}

		public DataBlockFactory<T> getDataBlockFactory() {

			return dataBlockFactory;
		}

		public DataCodec<T> getDataCodec() {

			return dataCodec;
		}

		public BytesCodec[] getCodecs() {

			return codecs;
		}
	}

	static abstract class AbstractN5DataBlockCodec<T> extends AbstractDataBlockCodec<T> {

		private static final int VAR_OBJ_BYTES_PER_ELEMENT = 1;

		AbstractN5DataBlockCodec(
				final DataCodec<T> dataCodec,
				final DataBlockFactory<T> dataBlockFactory,
				final BytesCodec... codecs) {

			super(dataCodec, dataBlockFactory, codecs);
		}

		protected abstract BlockHeader encodeBlockHeader(final DataBlock<T> dataBlock, ReadData blockData) throws IOException;

		@Override public ReadData encode(DataBlock<T> dataBlock) throws IOException {

			return ReadData.from(out -> {

				final ReadData dataReadData = getDataCodec().serialize(dataBlock.getData());
				final ReadData encodedData = new ConcatenatedBytesCodec(getCodecs()).encode(dataReadData);
				final BlockHeader header = encodeBlockHeader(dataBlock, dataReadData);

				header.writeTo(out);
				encodedData.writeTo(out);
			});
		}

		protected abstract BlockHeader decodeBlockHeader(final InputStream in) throws IOException;

		@Override
		public DataBlock<T> decode(final ReadData readData, final long[] gridPosition) throws IOException {

			try (final InputStream in = readData.inputStream()) {
				final BlockHeader header = decodeBlockHeader(in);

				final int bytesPerElement
						= getDataCodec().bytesPerElement() == -1
						? VAR_OBJ_BYTES_PER_ELEMENT
						: getDataCodec().bytesPerElement();

				final int numElements = header.numElements();
				final ReadData blockData = ReadData.from(in, numElements * bytesPerElement);
				final ReadData decodeData = new ConcatenatedBytesCodec(getCodecs()).decode(blockData);
				final T data = getDataCodec().deserialize(decodeData, numElements);
				return getDataBlockFactory().createDataBlock(header.blockSize(), gridPosition, data);
			}
		}
	}

	/**
	 * DataBlockCodec for all N5 data types, except STRING and OBJECT
	 */
	static class DefaultDataBlockCodec<T> extends AbstractN5DataBlockCodec<T> {

		DefaultDataBlockCodec(
				final DataCodec<T> dataCodec,
				final DataBlockFactory<T> dataBlockFactory,
				final BytesCodec... codecs) {

			super(dataCodec, dataBlockFactory, codecs);
		}

		@Override
		protected BlockHeader encodeBlockHeader(final DataBlock<T> dataBlock, ReadData blockData) throws IOException {

			return new BlockHeader(dataBlock.getSize(), dataBlock.getNumElements());
		}

		@Override
		protected BlockHeader decodeBlockHeader(final InputStream in) throws IOException {

			return BlockHeader.readFrom(in, MODE_DEFAULT, MODE_VARLENGTH);
		}
	}

	/**
	 * DataBlockCodec for N5 data type STRING
	 */
	static class StringDataBlockCodec extends AbstractN5DataBlockCodec<String[]> {

		public StringDataBlockCodec(
				final DataCodec<String[]> dataCodec,
				final DataBlockFactory<String[]> dataBlockFactory,
				final BytesCodec... codecs) {

			super(dataCodec, dataBlockFactory, codecs);
		}

		@Override
		protected BlockHeader encodeBlockHeader(final DataBlock<String[]> dataBlock, ReadData blockData) throws IOException {

			return new BlockHeader(dataBlock.getSize(), (int)blockData.length());
		}

		@Override
		protected BlockHeader decodeBlockHeader(final InputStream in) throws IOException {

			return BlockHeader.readFrom(in, MODE_DEFAULT, MODE_VARLENGTH);
		}
	}

	/**
	 * DataBlockCodec for N5 data type OBJECT
	 */
	static class ObjectDataBlockCodec extends AbstractN5DataBlockCodec<byte[]> {

		public ObjectDataBlockCodec(
				final DataCodec<byte[]> dataCodec,
				final DataBlockFactory<byte[]> dataBlockFactory,
				final BytesCodec... codecs) {

			super(dataCodec, dataBlockFactory, codecs);
		}

		@Override protected BlockHeader encodeBlockHeader(DataBlock<byte[]> dataBlock, ReadData blockData) throws IOException {

			return new BlockHeader(null, dataBlock.getNumElements());
		}

		@Override
		protected BlockHeader decodeBlockHeader(final InputStream in) throws IOException {

			return BlockHeader.readFrom(in, MODE_OBJECT);
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
				throw new IOException("Unexpected mode: " + mode);
			}

			boolean modeIsOk = allowedModes == null || allowedModes.length == 0;
			for (int i = 0; !modeIsOk && i < allowedModes.length; ++i) {
				if (mode == allowedModes[i]) {
					modeIsOk = true;
					break;
				}
			}
			if (!modeIsOk) {
				throw new IOException("Unexpected mode: " + mode);
			}

			return new BlockHeader(mode, blockSize, numElements);
		}
	}
}
