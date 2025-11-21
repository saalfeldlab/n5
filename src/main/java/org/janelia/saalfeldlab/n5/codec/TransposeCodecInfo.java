package org.janelia.saalfeldlab.n5.codec;

import org.janelia.saalfeldlab.n5.serialization.NameConfig;

@NameConfig.Name(value = TransposeCodecInfo.TYPE)
public class TransposeCodecInfo<T> implements DatasetCodecInfo {

	public static final String TYPE = "transpose";

	@NameConfig.Parameter
	private int[] order;

	public TransposeCodecInfo() {
		// for serialization
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public int[] getOrder() {

		return order;
	}

	@Override
	public DatasetCodec create() {

		// TODO
		return null;
	}

}