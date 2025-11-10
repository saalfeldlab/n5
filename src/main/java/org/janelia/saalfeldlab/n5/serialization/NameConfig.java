/*-
 * #%L
 * Not HDF5
 * %%
 * Copyright (C) 2017 - 2025 Stephan Saalfeld
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package org.janelia.saalfeldlab.n5.serialization;

import org.scijava.annotations.Indexable;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Configuration interface for N5 serialization naming and parameter annotations.
 * <p>
 * This interface provides a standardized way to configure serialization names and parameters
 * for N5 components such as compression algorithms and codecs. It defines annotations that
 * control how classes and their fields are serialized and deserialized for the N5 API.
 * <p>
 * Classes implementing this interface can use the provided annotations to:
 * <ul>
 *   <li>Define a serialization type name with {@link Name @Name}</li>
 *   <li>Specify a namespace prefix with {@link Prefix @Prefix}</li>
 *   <li>Mark fields as serialization parameters with {@link Parameter @Parameter}</li>
 * </ul>
 * 
 * @see Name
 * @see Prefix
 * @see Parameter
 */
public interface NameConfig extends Serializable {

	/**
	 * Defines a namespace prefix for serialization.
	 * <p>
	 * This annotation specifies a prefix that is prepended to the serialization
	 * type name, creating a namespaced identifier. This is useful for organizing
	 * related components into logical groups, usually all the components implementing
	 * a particular interface.
	 * 
	 * @see Name
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.TYPE)
	@interface Prefix {
		String value();
	}

	/**
	 * Specifies the serialization type name for a class.
	 * <p>
	 * This annotation defines the string identifier used during serialization and
	 * deserialization to identify the type. The name should be unique within its
	 * namespace and is typically a short, descriptive identifier.
	 * 
	 * @see Prefix
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.TYPE)
	@Indexable
	@interface Name {
		String value();
	}

	/**
	 * Controls whether a class should be serializable as a {@code NameConfig}.
	 * <p>
	 * This annotation allows explicitly enabling or disabling serialization for a class.
	 * <p>
	 * By default, classes are serialized.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.TYPE)
	@Indexable @interface Serialize {
		boolean value() default true;
	}

	/**
	 * Marks a field as a parameter to be serialized.
	 * <p>
	 * This annotation identifies fields that should be included during serialization
	 * and deserialization. It supports both required and optional parameters.
	 * <p>
	 * The {@code value} attribute can be used to specify an alternative name for
	 * the parameter during serialization. If not specified, the field name is used.
	 * 
	 * @see Name
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.FIELD)
	@interface Parameter {
		/**
		 * Alternative name for the parameter during serialization.
		 * If empty, the field name is used.
		 * 
		 * @return the parameter name, or empty string for field name
		 */
		String value() default "";
		
		/**
		 * Whether this parameter is optional.
		 * Optional parameters may be omitted during deserialization.
		 * 
		 * @return {@code true} if the parameter is optional, {@code false} otherwise
		 */
		boolean optional() default false;
	}

	/**
	 * Returns the serialization type name for this instance.
	 * <p>
	 * This method retrieves the value from the {@link Name @Name} annotation
	 * if present on the class.
	 * 
	 * @return the type name from the {@code @Name} annotation, or {@code null} if not annotated
	 */
	default String getType() {

		final Name type = getClass().getAnnotation(Name.class);
		return type == null ? null : type.value();

	}
}

