package org.janelia.saalfeldlab.n5;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.janelia.saalfeldlab.n5.codec.DataCodec;
import org.janelia.saalfeldlab.n5.codec.CodecInfo;
import org.janelia.saalfeldlab.n5.codec.DataCodecInfo;
import org.scijava.annotations.Indexable;

/**
 * This interface is used to indicate that a {@link DataCodec} can be
 * serialized as a "compression" for the N5 format (using the N5 API).
 * <p>
 * N5Readers and N5Writers for the N5 format can declare DataCodecs that
 * implement this interface so that the {@link CompressionAdapter} is used for
 * serialization.
 * <p>
 * See also: an alternative method for serializing general {@link CodecInfo}s is
 * with the {@link NameConfigAdapter}. This interface remains for legacy
 * (de)serialization.
 *
 * @author Stephan Saalfeld
 */
public interface Compression extends Serializable, DataCodec, DataCodecInfo {

	/**
	 * Annotation for runtime discovery of compression schemes.
	 *
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.TYPE)
	@Indexable
	@interface CompressionType {

		String value();
	}

	/**
	 * Annotation for runtime discovery of compression schemes.
	 *
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.FIELD)
	@interface CompressionParameter {}

	default String getType() {

		final CompressionType compressionType = getClass().getAnnotation(CompressionType.class);
		if (compressionType == null)
			return null;
		else
			return compressionType.value();
	}

	@Override
	default DataCodec create() {
		return this;
	}

}
