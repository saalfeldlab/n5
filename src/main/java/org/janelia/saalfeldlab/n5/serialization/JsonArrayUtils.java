package org.janelia.saalfeldlab.n5.serialization;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class JsonArrayUtils {

	public static void reverse(final JsonArray array) {

		JsonElement a;
		final int max = array.size() - 1;
		for (int i = (max - 1) / 2; i >= 0; --i) {
			final int j = max - i;
			a = array.get(i);
			array.set(i, array.get(j));
			array.set(j, a);
		}
	}

}
