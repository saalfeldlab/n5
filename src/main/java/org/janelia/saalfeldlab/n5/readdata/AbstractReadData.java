package org.janelia.saalfeldlab.n5.readdata;

import java.nio.ByteOrder;

abstract class AbstractReadData implements ReadData {

	private ByteOrder byteOrder;

	AbstractReadData() {
		this(ByteOrder.BIG_ENDIAN);
	}

	AbstractReadData(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}

	@Override
	public ByteOrder order() {
		return byteOrder;
	}

	@Override
	public ReadData order(final ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
		return this;
	}
}
