package org.janelia.saalfeldlab.n5;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

class ReflectionUtils {

	static <T> void setFieldValue(
			final Object object,
			final String fieldName,
			final T value) throws NoSuchFieldException, IllegalAccessException {

		Field modifiersField;
		boolean isModifiersAccessible;
		try {
			modifiersField = Field.class.getDeclaredField("modifiers");
			isModifiersAccessible = modifiersField.isAccessible();
			modifiersField.setAccessible(true);
		} catch (final NoSuchFieldException e) {
			// Java 11+ does not allow to access modifiers
			modifiersField = null;
			isModifiersAccessible = false;
		}

		final Field field = object.getClass().getDeclaredField(fieldName);
		final boolean isFieldAccessible = field.isAccessible();
		field.setAccessible(true);

		if (modifiersField != null) {
			final int modifiers = field.getModifiers();
			modifiersField.setInt(field, modifiers & ~Modifier.FINAL);
			field.set(object, value);
			modifiersField.setInt(field, modifiers);
		} else {
			field.set(object, value);
		}

		field.setAccessible(isFieldAccessible);
		if (modifiersField != null) {
			modifiersField.setAccessible(isModifiersAccessible);
		}
	}
}
