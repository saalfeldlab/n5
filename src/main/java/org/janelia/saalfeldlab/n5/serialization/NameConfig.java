package org.janelia.saalfeldlab.n5.serialization;

import org.scijava.annotations.Indexable;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public interface NameConfig extends Serializable {

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
	@interface Name {
		String value();
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.TYPE)
	@Indexable
	@interface Serialize {
		boolean value() default true;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Inherited
	@Target(ElementType.FIELD)
	@interface Parameter {
		String value() default "";
		boolean optional() default false;
	}

	default String getType() {

		final Name type = getClass().getAnnotation(Name.class);
		return type == null ? null : type.value();

	}
}

