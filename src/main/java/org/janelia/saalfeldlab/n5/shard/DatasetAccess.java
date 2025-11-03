package org.janelia.saalfeldlab.n5.shard;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.shard.Nesting.NestedGrid;

import java.util.List;

/**
 * Wrap an instantiated DataBlock/shard codec hierarchy to implement (single and
 * batch) DataBlock read/write methods.
 *
 * @param <T>
 * 		type of the data contained in the DataBlock
 */
public interface DatasetAccess<T> {

	DataBlock<T> readBlock(PositionValueAccess kva, long[] gridPosition) throws N5IOException;

	void writeBlock(PositionValueAccess kva, DataBlock<T> dataBlock) throws N5IOException;

	boolean deleteBlock(PositionValueAccess kva, long[] gridPosition) throws N5IOException;

	List<DataBlock<T>> readBlocks(PositionValueAccess kva, List<long[]> positions);

	void writeBlocks(PositionValueAccess kva, List<DataBlock<T>> blocks);

	NestedGrid getGrid();
}
