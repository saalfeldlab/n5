package org.janelia.saalfeldlab.n5;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.Iterator;

public abstract class LinkedAttributePathToken<T extends JsonElement> implements Iterator<LinkedAttributePathToken<?>> {

	/**
	 * The JsonElement which contains the mapping that this token represents.
	 */
	protected T parentJson;

	/**
	 * The token representing the subsequent path element.
	 */
	protected LinkedAttributePathToken<?> childToken;

	/**
	 * @return a reference object of type {@link T}
	 */
	public abstract T getJsonType();

	/**
	 * This method will retgurn the child element, if the {@link #parentJson} contains a child that
	 * is valid for this token. If the {@link #parentJson} does not have a child, this method will
	 * create it. If the {@link #parentJson} does have a child, but it is not {@link #jsonCompatible(JsonElement)}
	 * with our {@link #childToken}, then this method will also create a new (compatible) child, and replace
	 * the existing incompatible child.
	 *
	 * @return the resulting child element
	 */
	public abstract JsonElement getOrCreateChildElement();

	/**
	 * This method will write into {@link #parentJson} the subsequent {@link JsonElement}.
	 * <br>
	 * The written JsonElement will EITHER be:
	 * <ul>
	 *     <li>The result of serializing {@code value} ( if {@link #childToken} is {@code null} )</li>
	 *     <li>The {@link JsonElement} which represents our {@link #childToken} (See {@link #getOrCreateChildElement()} )</li>
	 * </ul>
	 *
	 * @param gson  instance used to serialize {@code value }
	 * @param value to write
	 * @return the object that was written.
	 */
	public JsonElement writeChild(Gson gson, Object value) {

		if (childToken != null)
			/* If we have a child, get/create it and set current json element to it */
			return getOrCreateChildElement();
		else {
			/* We are done, no token remaining */
			writeValue(gson, value);
			return getChildElement();
		}
	}

	/**
	 * Check if the provided {@code json} is compatible with this token.
	 * <br>
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

	/**
	 * Write {@code value} into {@link #parentJson}.
	 * <br>
	 * Should only be called when {@link #childToken} is null (i.e. this is the last token in the attribute path).
	 *
	 * @param gson instance used to serialize {@code value}
	 * @param value to serialize
	 */
	protected abstract void writeValue(Gson gson, Object value);

	/**
	 * Write the {@link JsonElement} the corresponds to {@link #childToken} into {@link #parentJson}.
	 */
	protected abstract void writeChildElement();

	/**
	 * @return the element that is represented by {@link #childToken} if present.
	 */
	protected abstract JsonElement getChildElement();

	/**
	 * If {@code json} is compatible with the type or token that we are, then set {@link #parentJson} to {@code json}.
	 * <br>
	 * However, if {@code json} is either {@code null} or not {@link #jsonCompatible(JsonElement)} then
	 * {@link #parentJson} will be set to a new instance of {@link T}.
	 *
	 * @param json to attempt to set to {@link #parentJson}
	 * @return the value set to {@link #parentJson}.
	 */
	protected JsonElement setAndCreateParentElement(JsonElement json) {

		if (json == null || !jsonCompatible(json)) {
			//noinspection unchecked
			parentJson = (T)getJsonType().deepCopy();
		} else {
			//noinspection unchecked
			parentJson = (T)json;
		}
		return parentJson;
	}

	/**
	 * @return if we have a {@link #childToken}.
	 */
	@Override public boolean hasNext() {

		return childToken != null;
	}

	/**
	 * @return {@link #childToken}
	 */
	@Override public LinkedAttributePathToken<?> next() {

		return childToken;
	}

	public static class ObjectAttributeToken extends LinkedAttributePathToken<JsonObject> {

		private static final JsonObject JSON_OBJECT = new JsonObject();

		private final String key;

		public ObjectAttributeToken(String key) {

			this.key = key;
		}

		/**
		 * @return the {@link #key} this token maps it's {@link #getChildElement()} to.
		 */
		public String getKey() {

			return key;
		}

		@Override public String toString() {

			return getKey();
		}

		@Override public JsonObject getJsonType() {

			return JSON_OBJECT;
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

		/**
		 * @return the {@link #index} this token maps it's {@link #getChildElement()} to.
		 */
		public int getIndex() {

			return index;
		}

		@Override public String toString() {

			return "[" + getIndex() + "]";
		}

		@Override public JsonArray getJsonType() {

			return JSON_ARRAY;
		}

		@Override public JsonElement getOrCreateChildElement() {
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
		 * @param value we wish to check if represents a {@link Number} or not.
		 * @return true if {@code value} represents a {@link Number}
		 */
		private static boolean valueRepresentsANumber(Object value) {

			return value instanceof Number || (value instanceof JsonPrimitive && ((JsonPrimitive)value).isNumber());
		}
	}
}

