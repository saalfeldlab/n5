package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import java.io.IOException;

/**
 * De/serialize {@link DataBlock} from/to {@link ReadData}.
 *
 * @param <T> type of the data contained in the DataBlock
 */
public interface DataBlockCodec<T> {

	ReadData encode(DataBlock<T> dataBlock) throws IOException;

	DataBlock<T> decode(ReadData readData, long[] gridPosition) throws IOException;
}
