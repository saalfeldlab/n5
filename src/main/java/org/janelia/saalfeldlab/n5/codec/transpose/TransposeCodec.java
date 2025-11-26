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

}
