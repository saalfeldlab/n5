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

	private static final BlockCodecFactory<byte[]>   BYTE   = (byteOrder, blockSize, codec) -> new RawBlockCodec<>(FlatArrayCodec.BYTE, ByteArrayDataBlock::new, blockSize, codec);
	private static final BlockCodecFactory<short[]>  SHORT  = (byteOrder, blockSize, codec) -> new RawBlockCodec<>(FlatArrayCodec.SHORT(byteOrder), ShortArrayDataBlock::new, blockSize, codec);
	private static final BlockCodecFactory<int[]>    INT    = (byteOrder, blockSize, codec) -> new RawBlockCodec<>(FlatArrayCodec.INT(byteOrder), IntArrayDataBlock::new, blockSize, codec);
	private static final BlockCodecFactory<long[]>   LONG   = (byteOrder, blockSize, codec) -> new RawBlockCodec<>(FlatArrayCodec.LONG(byteOrder), LongArrayDataBlock::new, blockSize, codec);
	private static final BlockCodecFactory<float[]>  FLOAT  = (byteOrder, blockSize, codec) -> new RawBlockCodec<>(FlatArrayCodec.FLOAT(byteOrder), FloatArrayDataBlock::new, blockSize, codec);
	private static final BlockCodecFactory<double[]> DOUBLE = (byteOrder, blockSize, codec) -> new RawBlockCodec<>(FlatArrayCodec.DOUBLE(byteOrder), DoubleArrayDataBlock::new, blockSize, codec);
	private static final BlockCodecFactory<String[]> STRING = (byteOrder, blockSize, codec) -> new RawBlockCodec<>(FlatArrayCodec.ZARR_STRING, StringDataBlock::new,  blockSize, codec);
	private static final BlockCodecFactory<byte[]>   OBJECT = (byteOrder, blockSize, codec) -> new RawBlockCodec<>(FlatArrayCodec.OBJECT, ByteArrayDataBlock::new, blockSize, codec);

	private RawBlockCodecs() {}

	public static <T> BlockCodec<T> create(
			final DataType dataType,
			final ByteOrder byteOrder,
			final int[] blockSize,
			final DataCodec codec) {
		final BlockCodecFactory<?> factory;
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
		// TODO: What about OBJECT?
		default:
			throw new IllegalArgumentException("Unsupported data type: " + dataType);
		}
		final BlockCodecFactory<T> tFactory = (BlockCodecFactory<T>) factory;
		return tFactory.create(byteOrder, blockSize, codec);
	}

	private interface BlockCodecFactory<T> {

		/**
		 * Create a {@link BlockCodec} that uses the specified {@code ByteOrder}
		 * and {@code DataCodec} and de/serializes {@code DataBlock<T>} of the
		 * specified {@code blockSize} to raw format.
		 *
		 * @return Raw {@code BlockCodec}
		 */
		BlockCodec<T> create(ByteOrder byteOrder, int[] blockSize, DataCodec dataCodec);
	}

	private static class RawBlockCodec<T> implements BlockCodec<T> {

		private final FlatArrayCodec<T> dataCodec;
		private final DataBlock.DataBlockFactory<T> dataBlockFactory;
		private final int[] blockSize;
		private final int numElements;
		private final DataCodec codec;

		RawBlockCodec(
				final FlatArrayCodec<T> dataCodec,
				final DataBlock.DataBlockFactory<T> dataBlockFactory,
				final int[] blockSize,
				final DataCodec codec) {

			this.dataCodec = dataCodec;
			this.dataBlockFactory = dataBlockFactory;
			this.blockSize = blockSize;
			this.numElements = DataBlock.getNumElements(blockSize);
			this.codec = codec;
		}

		@Override
		public ReadData encode(DataBlock<T> dataBlock) {

			return ReadData.from(out -> {
				final ReadData blockData = dataCodec.encode(dataBlock.getData());
				codec.encode(blockData).writeTo(out);
			});
		}

		@Override
		public DataBlock<T> decode(ReadData readData, long[] gridPosition) {

			final ReadData decodeData = codec.decode(readData);
			final T data = dataCodec.decode(decodeData, numElements);
			return dataBlockFactory.createDataBlock(blockSize, gridPosition, data);
		}
	}
}
