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
package org.janelia.saalfeldlab.n5.cache;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class N5CacheTest {


	@Test
	public void cacheBackingTest() {

		final DummyBackingStorage backingStorage = new DummyBackingStorage();

		final N5JsonCache cache = new N5JsonCache(backingStorage);

		int expectedAttrCallCount = 0;

		// check existence, ensure backing storage is only called once
		//	this cache `exists` is overridden to write an attribute
		//	which means the `exists` call checks attr existence first,
		//	and since it finds some, it infers the existence without
		//	an explicit check. Some backends don't support exists
		//	so this is a way to handle those cases more elegantly
		assertEquals(0, backingStorage.existsCallCount);
		cache.exists("a", null);
		assertEquals(++expectedAttrCallCount, backingStorage.attrCallCount);
		assertEquals(0, backingStorage.existsCallCount);
		cache.exists("a", null);
		assertEquals(0, backingStorage.existsCallCount);
		assertEquals(expectedAttrCallCount, backingStorage.attrCallCount);

		// check existence of new group, ensure backing storage is only called one more time
		cache.exists("b", null);
		assertEquals(++expectedAttrCallCount, backingStorage.attrCallCount);
		assertEquals(0, backingStorage.existsCallCount);
		cache.exists("b", null);
		assertEquals(expectedAttrCallCount, backingStorage.attrCallCount);
		assertEquals(0, backingStorage.existsCallCount);

		// check isDataset, ensure backing storage is only called when expected
		// isDataset is called by exists, so should have been called twice here
		assertEquals(2, backingStorage.isDatasetCallCount);
		cache.isDataset("a", null);
		assertEquals(2, backingStorage.isDatasetCallCount);

		assertEquals(2, backingStorage.isDatasetCallCount);
		cache.isDataset("b", null);
		assertEquals(2, backingStorage.isDatasetCallCount);

		// check isGroup, ensure backing storage is only called when expected
		// isGroup is called by exists, so should have been called twice here
		assertEquals(2, backingStorage.isGroupCallCount);
		cache.isDataset("a", null);
		assertEquals(2, backingStorage.isGroupCallCount);

		assertEquals(2, backingStorage.isGroupCallCount);
		cache.isDataset("b", null);
		assertEquals(2, backingStorage.isGroupCallCount);

		// similarly check list, ensure backing storage is only called when expected
		// list is called by exists, so should have been called twice here
		assertEquals(0, backingStorage.listCallCount);
		cache.list("a");
		assertEquals(1, backingStorage.listCallCount);

		assertEquals(1, backingStorage.listCallCount);
		cache.list("b");
		assertEquals(2, backingStorage.listCallCount);

		// finally check getAttributes
		// it is not called by exists (since it needs the cache key)
		assertEquals(expectedAttrCallCount, backingStorage.attrCallCount);
		cache.getAttributes("a", "foo");
		assertEquals(++expectedAttrCallCount, backingStorage.attrCallCount);
		cache.getAttributes("a", "foo");
		assertEquals(expectedAttrCallCount, backingStorage.attrCallCount);
		cache.getAttributes("a", "bar");
		assertEquals(++expectedAttrCallCount, backingStorage.attrCallCount);
		cache.getAttributes("a", "bar");
		assertEquals(expectedAttrCallCount, backingStorage.attrCallCount);
		cache.getAttributes("a", "face");
		assertEquals(++expectedAttrCallCount, backingStorage.attrCallCount);

		cache.getAttributes("b", "foo");
		assertEquals(++expectedAttrCallCount, backingStorage.attrCallCount);
		cache.getAttributes("b", "foo");
		assertEquals(expectedAttrCallCount, backingStorage.attrCallCount);
		cache.getAttributes("b", "bar");
		assertEquals(++expectedAttrCallCount, backingStorage.attrCallCount);
		cache.getAttributes("b", "bar");
		assertEquals(expectedAttrCallCount, backingStorage.attrCallCount);
		cache.getAttributes("b", "face");
		assertEquals(++expectedAttrCallCount, backingStorage.attrCallCount);

	}

	protected static class DummyBackingStorage implements N5JsonCacheableContainer {

		int attrCallCount = 0;
		int existsCallCount = 0;
		int isGroupCallCount = 0;
		int isDatasetCallCount = 0;
		int isGroupFromAttrsCallCount = 0;
		int isDatasetFromAttrsCallCount = 0;
		int listCallCount = 0;

		public DummyBackingStorage() {
		}

		public JsonElement getAttributesFromContainer(final String key, final String cacheKey) {
			attrCallCount++;
			final JsonObject obj = new JsonObject();
			obj.addProperty("key", "value");
			return obj;
		}

		public boolean existsFromContainer(final String path, final String cacheKey) {
			existsCallCount++;
			return true;
		}

		public boolean isGroupFromContainer(final String key) {
			isGroupCallCount++;
			return true;
		}

		public boolean isDatasetFromContainer(final String key) {
			isDatasetCallCount++;
			return true;
		}

		public String[] listFromContainer(final String key) {
			listCallCount++;
			return new String[] { "list" };
		}

		@Override
		public boolean isGroupFromAttributes(final String cacheKey, final JsonElement attributes) {
			isGroupFromAttrsCallCount++;
			return true;
		}

		@Override
		public boolean isDatasetFromAttributes(final String cacheKey, final JsonElement attributes) {
			isDatasetFromAttrsCallCount++;
			return true;
		}
	}

}
