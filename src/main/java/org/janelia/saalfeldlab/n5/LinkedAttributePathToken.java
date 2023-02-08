package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.Iterator;

public abstract class LinkedAttributePathToken<T extends JsonElement> implements Iterator<LinkedAttributePathToken<?>> {

	protected T parentJson;
	protected LinkedAttributePathToken<?> childToken;

	public void setChildToken(LinkedAttributePathToken<?> childToken) {

		this.childToken = childToken;
	}

	public T getParentJson() {

		return parentJson;
	}

	public abstract T getJsonType();

	public abstract boolean canNavigate(JsonElement parent);

	public abstract JsonElement getOrCreateChildElement();

	public JsonElement writeChild(Gson gson, Object value) {

		if (childToken != null)
			/* If we have a child, get/create it and set current json element to it */
			return getOrCreateChildElement();
		else
			/* We are done, no token remaining */
			return writeJsonElement(gson, value);
	}

	public JsonElement writeJsonElement(Gson gson, Object value) {

		if (hasNext()) {
			writeChildElement();
		} else {
			writeValue(gson, value);
		}
		return getChildElement();
	}

	/**
	 * Check if the provided {@code json} is compatible with this token.
	 * <p>
	 * Compatibility means thath the provided {@code JsonElement} does not
	 * explicitly conflict with this token. That means that {@code null} is
	 * always compatible. The only real incompatibility is when the {@code JsonElement}
	 * that we receive is different that we expect.
	 *
	 * @param json element that we are testing for compatibility.
	 * @return false if {@code json} is not null and is not of type {@code T}
	 */
	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	public boolean jsonCompatible(JsonElement json) {

		return json == null || json.getClass() == getJsonType().getClass();
	}

	protected abstract void writeValue(Gson gson, Object value);

	protected abstract void writeChildElement();

	protected abstract JsonElement getChildElement();

	protected JsonElement setAndCreateParentElement(JsonElement json) {
		if (json == null || !jsonCompatible(json)) {
			//noinspection unchecked
			parentJson = (T) getJsonType().deepCopy();
		} else {
			//noinspection unchecked
			parentJson = (T) json;
		}
		return parentJson;
	}

	@Override public boolean hasNext() {

		return childToken != null;
	}

	@Override public LinkedAttributePathToken<?> next() {

		return childToken;
	}

	public static class ObjectAttributeToken extends LinkedAttributePathToken<JsonObject> {

		private static final JsonObject JSON_OBJECT = new JsonObject();

		public final String key;

		public ObjectAttributeToken(String key) {

			this.key = key;
		}

		public String getKey() {

			return key;
		}

		@Override public String toString() {

			return getKey();
		}

		@Override public JsonObject getJsonType() {

			return JSON_OBJECT;
		}

		@Override public boolean canNavigate(JsonElement json) {

			return json != null && json.isJsonObject() && json.getAsJsonObject().has(key);
		}

		@Override public JsonElement getOrCreateChildElement() {

			final JsonElement childElement = parentJson.getAsJsonObject().get(key);
			if (!parentJson.has(key) || childToken != null && !childToken.jsonCompatible(childElement)) {
				writeChildElement();
			}
			return getChildElement();
		}

		@Override protected JsonElement getChildElement() {

			return parentJson.get(key);
		}

		@Override protected void writeValue(Gson gson, Object value) {

			parentJson.add(key, gson.toJsonTree(value));
		}

		@Override protected void writeChildElement() {

			if (childToken != null)
				parentJson.add(key, childToken.getJsonType().deepCopy());

		}

	}

	public static class ArrayAttributeToken extends LinkedAttributePathToken<JsonArray> {

		private static final JsonArray JSON_ARRAY = new JsonArray();

		private final int index;

		public ArrayAttributeToken(int index) {

			this.index = index;
		}

		public int getIndex() {

			return index;
		}

		@Override public String toString() {

			return "[" + getIndex() + "]";
		}

		@Override public JsonArray getJsonType() {

			return JSON_ARRAY;
		}

		@Override public boolean canNavigate(JsonElement parent) {

			return parent != null && parent.isJsonArray() && parent.getAsJsonArray().size() > index;
		}

		@Override public JsonElement getOrCreateChildElement() {

			/* Two cases which required writing the child:
			* 	- The child element doesn't exist (in this case, there is nothing at the specified index).
			*	- The child element is incompatible with the child token (replaces the existing child element). */
			if (index >= parentJson.size() || childToken != null && !childToken.jsonCompatible(parentJson.get(index)))
				writeChildElement();

			return parentJson.get(index);
		}

		@Override protected void writeChildElement() {

			if (childToken != null) {
				fillArrayToIndex(parentJson, index, null);
				parentJson.set(index, childToken.getJsonType().deepCopy());
			}
		}

		@Override protected JsonElement getChildElement() {

			return parentJson.get(index);
		}

		@Override protected void writeValue(Gson gson, Object value) {

			fillArrayToIndex(parentJson, index, value);
			parentJson.set(index, gson.toJsonTree(value));
		}

		/**
		 * Fill {@code array} up to, and including, {@code index} with a default value, determined by the type of {@code value}.
		 * Importantly, this does NOT set {@code array[index]=value}.
		 *
		 * @param array to fill
		 * @param index to fill the array to (inclusive)
		 * @param value used to determine the default array fill value
		 */
		private static void fillArrayToIndex(JsonArray array, int index, Object value) {

			final JsonElement fillValue;
			if (valueRepresentsANumber(value)) {
				fillValue = new JsonPrimitive(0);
			} else {
				fillValue = JsonNull.INSTANCE;
			}

			for (int i = array.size(); i <= index; i++) {
				array.add(fillValue);
			}
		}

		/**
		 * Check if {@value} represents a {@link Number}.
		 * True if {@code value} either:
		 * <ul>
		 *     <li>is a {@code Number}, or;</li>
		 *     <li>is a {@link JsonPrimitive} where {@link JsonPrimitive#isNumber()}</li>
		 * </ul>
		 *
		 *
		 * @param value we wish to check if represents a {@link Number} or not.
		 * @return true if {@code value} represents a {@link Number}
		 */
		private static boolean valueRepresentsANumber(Object value) {

			return value instanceof Number || (value instanceof JsonPrimitive && ((JsonPrimitive)value).isNumber());
		}
	}
}

