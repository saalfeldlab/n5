package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.ByteArrayDataBlock;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DoubleArrayDataBlock;
import org.janelia.saalfeldlab.n5.FloatArrayDataBlock;
import org.janelia.saalfeldlab.n5.IntArrayDataBlock;
import org.janelia.saalfeldlab.n5.LongArrayDataBlock;
import org.janelia.saalfeldlab.n5.ShortArrayDataBlock;
import org.janelia.saalfeldlab.n5.StringDataBlock;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;

public class RawBlockCodecs {

	public static final RawDataBlockCodecFactory<byte[]> BYTE = (byteOrder, blockSize, codecs) -> new RawDataBlockCodec<>(DataCodec.BYTE, ByteArrayDataBlock::new, blockSize, codecs);
	public static final RawDataBlockCodecFactory<short[]> SHORT = (byteOrder, blockSize, codecs) -> new RawDataBlockCodec<>(DataCodec.SHORT(byteOrder), ShortArrayDataBlock::new, blockSize, codecs);
	public static final RawDataBlockCodecFactory<int[]> INT = (byteOrder, blockSize, codecs) -> new RawDataBlockCodec<>(DataCodec.INT(byteOrder), IntArrayDataBlock::new, blockSize, codecs);
	public static final RawDataBlockCodecFactory<long[]> LONG = (byteOrder, blockSize, codecs) -> new RawDataBlockCodec<>(DataCodec.LONG(byteOrder), LongArrayDataBlock::new, blockSize, codecs);
	public static final RawDataBlockCodecFactory<float[]> FLOAT = (byteOrder, blockSize, codecs) -> new RawDataBlockCodec<>(DataCodec.FLOAT(byteOrder), FloatArrayDataBlock::new, blockSize, codecs);
	public static final RawDataBlockCodecFactory<double[]> DOUBLE = (byteOrder, blockSize, codecs) -> new RawDataBlockCodec<>(DataCodec.DOUBLE(byteOrder), DoubleArrayDataBlock::new, blockSize, codecs);
	public static final RawDataBlockCodecFactory<String[]> STRING =(byteOrder, blockSize, codecs) -> new RawDataBlockCodec<>(DataCodec.STRING(byteOrder), StringDataBlock::new,  blockSize, codecs);
	public static final RawDataBlockCodecFactory<byte[]> OBJECT =(byteOrder, blockSize, codecs) -> new RawDataBlockCodec<>(DataCodec.OBJECT, ByteArrayDataBlock::new, blockSize, codecs);

	private RawBlockCodecs() {}

	public static <T> DataBlockCodec<T> createDataBlockCodec(
			final DataType dataType,
			final ByteOrder byteOrder,
			final int[] blockSize,
			final Codec.BytesCodec codecs) {
		final RawDataBlockCodecFactory<?> factory;
		switch (dataType) {
		case UINT8:
		case INT8:
			factory = RawBlockCodecs.BYTE;
			break;
		case UINT16:
		case INT16:
			factory = RawBlockCodecs.SHORT;
			break;
		case UINT32:
		case INT32:
			factory = RawBlockCodecs.INT;
			break;
		case UINT64:
		case INT64:
			factory = RawBlockCodecs.LONG;
			break;
		case FLOAT32:
			factory = RawBlockCodecs.FLOAT;
			break;
		case FLOAT64:
			factory = RawBlockCodecs.DOUBLE;
			break;
		default:
			throw new IllegalArgumentException("Unsupported data type: " + dataType);
		}
		final RawDataBlockCodecFactory<T> tFactory = (RawDataBlockCodecFactory<T>)factory;
		return tFactory.createDataBlockCodec(byteOrder, blockSize, codecs);
	}

	public interface RawDataBlockCodecFactory<T> {

		DataBlockCodec<T> createDataBlockCodec(ByteOrder byteOrder, int[] blockSize, Codec.BytesCodec codecs);
	}

	private static class RawDataBlockCodec<T> extends N5Codecs.AbstractDataBlockCodec<T> {

		private final int[] blockSize;

		RawDataBlockCodec(
				final DataCodec<T> dataCodec,
				final DataBlock.DataBlockFactory<T> dataBlockFactory,
				final int[] blockSize,
				final Codec.BytesCodec codec) {

			super(dataCodec, dataBlockFactory, codec);
			this.blockSize = blockSize;
		}

		private int numElements() {
			if (blockSize.length == 0)
				return 0;

			int elements = 1;
			for (int dimLen : blockSize) {
				elements *= dimLen;
			}
			return elements;
		}

		@Override N5Codecs.BlockHeader createBlockHeader(DataBlock<T> dataBlock, ReadData blockData) throws IOException {

			return null;
		}

		@Override public ReadData encode(DataBlock<T> dataBlock) {

			return ReadData.from(out -> {
				final ReadData blockData = getDataCodec().serialize(dataBlock.getData());
				getCodec().encode(blockData).writeTo(out);
			});
		}

		@Override N5Codecs.BlockHeader decodeBlockHeader(InputStream in) throws IOException {

			return null;
		}

		@Override public DataBlock<T> decode(ReadData readData, long[] gridPosition) throws IOException {

			try (final InputStream in = readData.inputStream()) {
				final int bytesPerElement = getDataCodec().bytesPerElement();
				final ReadData blockData = ReadData.from(in, numElements() * bytesPerElement);
				final ReadData decodeData = getCodec().decode(blockData);

				final T data = getDataCodec().deserialize(decodeData, numElements());
				return getDataBlockFactory().createDataBlock(blockSize, gridPosition, data);
			}
		}
	}
}
