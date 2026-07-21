package org.janelia.saalfeldlab.n5.codec.dataset;

import java.util.Objects;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.codec.DatasetCodecInfo;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

/**
 * Describes an "array -&gt; array" transformation that subtracts an
 * {@code offset}, then multiplies by a {@code scale} factor.
 * <p>
 * Encoding: {@code out = (in - offset) * scale}
 * <br>
 * Decoding: {@code out = (in / scale) + offset}
 * <p>
 * Both {@code offset} and {@code scale} are optional and default to {@code 0}
 * and {@code 1}, respectively. This codec only rescales values; to actually
 * compress it is typically followed by a narrowing codec (e.g. {@code cast_value},
 * see {@link CastValueCodecInfo}).
 * <p>
 * See the specification of
 * <a href="https://github.com/zarr-developers/zarr-extensions/blob/main/codecs/scale_offset/README.md">Zarr's scale_offset codec</a>.
 */
@NameConfig.Name(value = ScaleOffsetCodecInfo.TYPE)
public class ScaleOffsetCodecInfo implements DatasetCodecInfo {

	private static final long serialVersionUID = -3495814872968404500L;

	public static final String TYPE = "scale_offset";

	public static final double DEFAULT_SCALE = 1;

	public static final double DEFAULT_OFFSET = 0;

	@NameConfig.Parameter(optional = true)
	private double scale;

	@NameConfig.Parameter(optional = true)
	private double offset;

	public ScaleOffsetCodecInfo() {
		// no-arg constructor for serialization
		this(DEFAULT_SCALE, DEFAULT_OFFSET);
	}

	public ScaleOffsetCodecInfo(final double scale, final double offset) {

		this.scale = scale;
		this.offset = offset;
	}

	@Override
	public String getType() {

		return TYPE;
	}

	public double getScale() {

		return scale;
	}

	public double getOffset() {

		return offset;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public ScaleOffsetCodec<?> create(final DatasetAttributes attributes) {

		return new ScaleOffsetCodec(attributes.getDataType(), scale, offset);
	}

	@Override
	public boolean equals(final Object obj) {

		if (obj instanceof ScaleOffsetCodecInfo) {
			final ScaleOffsetCodecInfo other = (ScaleOffsetCodecInfo)obj;
			return scale == other.scale && offset == other.offset;
		}
		return false;
	}

	@Override
	public int hashCode() {

		return Objects.hash(scale, offset);
	}

}
