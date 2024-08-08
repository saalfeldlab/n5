package org.janelia.saalfeldlab.n5.serialization;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.scijava.annotations.Indexable;

public interface N5NameConfig extends Serializable, N5Annotations {

	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.TYPE)
	@interface Prefix {
		String value();
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.TYPE)
	@Indexable
	@interface Type {
		String value();
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.FIELD)
	@interface Parameter {
		String value() default "";
	}

	default String getType() {

		final Type type = getClass().getAnnotation(Type.class);
		return type == null ? null : type.value();

	}
}

