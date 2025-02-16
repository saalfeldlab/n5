package org.janelia.saalfeldlab.n5.codec;

import java.io.IOException;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

public interface DataBlockCodec<T> {

	ReadData encode(DataBlock<T> dataBlock) throws IOException;

	DataBlock<T> decode(ReadData readData, long[] gridPosition) throws IOException;
}
