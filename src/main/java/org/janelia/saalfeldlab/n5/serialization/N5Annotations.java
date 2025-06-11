package org.janelia.saalfeldlab.n5.serialization;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Provides specialized annotations for N5 serialization behaviors.
 * <p>
 * This interface defines annotations that control specific serialization
 * transformations needed for N5 compatibility across different storage
 * formats, for example, when dealing with dimension ordering conventions.
 * 
 * @see ReverseArray
 */
public interface N5Annotations extends Serializable {

	/**
	 * Indicates that an array field should be reversed during serialization/deserialization.
	 * <p>
	 * This annotation is used to handle dimension ordering differences between storage formats.
	 * For example, Zarr uses C-order (row-major) dimension ordering [Z, Y, X], while N5 uses
	 * F-order (column-major) dimension ordering [X, Y, Z].
	 * <p>
	 * Example usage:
	 * <pre>{@code
	 * @ReverseArray
	 * @Parameter("chunk_shape")
	 * private final int[] shape;  // Will be reversed when serializing
	 * </pre>
	 * <p>
	 * This ensures that dimension-related arrays maintain the correct semantic meaning
	 * across different storage format conventions.
	 */
	@Inherited
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	@interface ReverseArray {
	}
}

