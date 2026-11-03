package org.janelia.saalfeldlab.n5.codec.string;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.codec.BlockCodec;
import org.janelia.saalfeldlab.n5.codec.BlockCodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.janelia.saalfeldlab.n5.codec.RawBlockCodecs;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * The zarr {@code vlen-utf8} codec: serializes blocks of variable-length UTF-8
 * strings. Only compatible with {@link DataType#STRING}.
 *
 * @see <a href="https://github.com/zarr-developers/zarr-extensions/tree/main/codecs/vlen-utf8">vlen-utf8</a>
 */
@NameConfig.Name(value = VlenUtf8BlockCodecInfo.TYPE)
public class VlenUtf8BlockCodecInfo implements BlockCodecInfo {

	private static final long serialVersionUID = 6093614730268445312L;

	public static final String TYPE = "vlen-utf8";

	@Override
	public String getType() {

		return TYPE;
	}

	@Override
	public <T> BlockCodec<T> create(final DataType dataType, final int[] blockSize, final DataCodecInfo... codecs) {

		if (dataType != DataType.STRING)
			throw new IllegalArgumentException(TYPE + " codec is only compatible with data type " + DataType.STRING
					+ ", but was " + dataType);

		// byte order is irrelevant for strings
		return RawBlockCodecs.create(dataType, null, blockSize, DataCodec.create(codecs));
	}

}
