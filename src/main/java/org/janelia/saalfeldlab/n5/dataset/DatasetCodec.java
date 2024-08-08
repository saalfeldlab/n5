package org.janelia.saalfeldlab.n5.dataset;

import org.janelia.saalfeldlab.n5.DataBlock;

/**
 * Represents a transformation of a dataset.
 *
 * Called an <a href="https://zarr-specs.readthedocs.io/en/latest/v3/core/v3.0.html#id18"> "array -> array codec" in zarr</href>
 */
public interface DatasetCodec {

	public <T> DataBlock<T> transform(final DataBlock<T> blockIn);

}
