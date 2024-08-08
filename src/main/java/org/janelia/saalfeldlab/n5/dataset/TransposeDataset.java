package org.janelia.saalfeldlab.n5.dataset;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.serialization.N5NameConfig;

@N5NameConfig.Type("transpose")
@N5NameConfig.Prefix("codec")
public class TransposeDataset implements DatasetCodec {

	private final int[] order;

	public TransposeDataset(final int[] order) {

		this.order = order;
	}

	private TransposeDataset() {

		this(null);
	}

	public int[] getOrder() {

		return order;
	}

	@Override
	public <T> DataBlock<T> transform(DataBlock<T> blockIn) {

		// TODO Auto-generated method stub
		return null;
	}

}
