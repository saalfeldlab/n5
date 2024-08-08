package org.janelia.saalfeldlab.n5.dataset;

import org.janelia.saalfeldlab.n5.DatasetAttributes;

public class AbstractDataset {

	public final DatasetAttributes attributes;

	public AbstractDataset(DatasetAttributes attributes) {

		this.attributes = attributes;
	}

}
