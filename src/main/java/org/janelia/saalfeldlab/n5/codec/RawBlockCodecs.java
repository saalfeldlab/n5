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

import java.nio.ByteOrder;

public class RawBlockCodecs {

	public static final RawDataBlockCodecFactory<byte[]> BYTE = (byteOrder, blockSize, codec) -> new RawDataBlockCodec<>(DataCodec.BYTE, ByteArrayDataBlock::new, blockSize, codec);
	public static final RawDataBlockCodecFactory<short[]> SHORT = (byteOrder, blockSize, codec) -> new RawDataBlockCodec<>(DataCodec.SHORT(byteOrder), ShortArrayDataBlock::new, blockSize, codec);
	public static final RawDataBlockCodecFactory<int[]> INT = (byteOrder, blockSize, codec) -> new RawDataBlockCodec<>(DataCodec.INT(byteOrder), IntArrayDataBlock::new, blockSize, codec);
	public static final RawDataBlockCodecFactory<long[]> LONG = (byteOrder, blockSize, codec) -> new RawDataBlockCodec<>(DataCodec.LONG(byteOrder), LongArrayDataBlock::new, blockSize, codec);
	public static final RawDataBlockCodecFactory<float[]> FLOAT = (byteOrder, blockSize, codec) -> new RawDataBlockCodec<>(DataCodec.FLOAT(byteOrder), FloatArrayDataBlock::new, blockSize, codec);
	public static final RawDataBlockCodecFactory<double[]> DOUBLE = (byteOrder, blockSize, codec) -> new RawDataBlockCodec<>(DataCodec.DOUBLE(byteOrder), DoubleArrayDataBlock::new, blockSize, codec);
	public static final RawDataBlockCodecFactory<String[]> STRING =(byteOrder, blockSize, codec) -> new RawDataBlockCodec<>(DataCodec.ZARR_STRING, StringDataBlock::new,  blockSize, codec);
	public static final RawDataBlockCodecFactory<byte[]> OBJECT =(byteOrder, blockSize, codec) -> new RawDataBlockCodec<>(DataCodec.OBJECT, ByteArrayDataBlock::new, blockSize, codec);

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
		case STRING:
			factory = RawBlockCodecs.STRING;
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

	private static class RawDataBlockCodec<T> implements DataBlockCodec<T> {

		private final DataCodec<T> dataCodec;
		private final DataBlock.DataBlockFactory<T> dataBlockFactory;
		private final int[] blockSize;
		private final Codec.BytesCodec codec;

		RawDataBlockCodec(
				final DataCodec<T> dataCodec,
				final DataBlock.DataBlockFactory<T> dataBlockFactory,
				final int[] blockSize,
				final Codec.BytesCodec codec) {

			this.dataCodec = dataCodec;
			this.dataBlockFactory = dataBlockFactory;
			this.blockSize = blockSize;
			this.codec = codec;
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

		@Override
		public ReadData encode(DataBlock<T> dataBlock) {

			return ReadData.from(out -> {
				final ReadData blockData = dataCodec.serialize(dataBlock.getData());
				codec.encode(blockData).writeTo(out);
			});
		}

		@Override
		public DataBlock<T> decode(ReadData readData, long[] gridPosition) {
			ReadData decodeData = codec.decode(readData);
			final T data = dataCodec.deserialize(decodeData, numElements());
			return dataBlockFactory.createDataBlock(blockSize, gridPosition, data);
		}
	}
}
