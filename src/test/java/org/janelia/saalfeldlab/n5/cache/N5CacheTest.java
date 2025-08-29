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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.janelia.saalfeldlab.n5.N5Exception;
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

	@Test
	public void testCopyOnReadPreventsExternalModification() {

		final DummyBackingStorage backingStorage = new DummyBackingStorage();
		final N5JsonCache cache = new N5JsonCache(backingStorage);
		
		// Get attributes and modify the returned object
		JsonElement attrs1 = cache.getAttributes("path", "key");
		attrs1.getAsJsonObject().addProperty("modified", "value");
		
		// Get attributes again - should not contain the modification
		JsonElement attrs2 = cache.getAttributes("path", "key");
		assertFalse(attrs2.getAsJsonObject().has("modified"));
		
		// Verify both calls return different instances
		assertNotSame(attrs1, attrs2);
	}

	@Test
	public void testCacheManipulationMethods() {

		final DummyBackingStorage backingStorage = new DummyBackingStorage();
		final N5JsonCache cache = new N5JsonCache(backingStorage);

		// First, ensure the path exists in cache
		assertTrue(cache.exists("path", null));

		// Test setAttributes
		JsonObject newAttrs = new JsonObject();
		newAttrs.addProperty("custom", "value");
		cache.setAttributes("path", "key", newAttrs);
		JsonElement retrievedAttrs = cache.getAttributes("path", "key");
		assertTrue(retrievedAttrs.getAsJsonObject().has("custom"));
		assertEquals("value", retrievedAttrs.getAsJsonObject().get("custom").getAsString());

		// Test updateCacheInfo
		JsonObject updatedAttrs = new JsonObject();
		updatedAttrs.addProperty("updated", "updated-value");
		cache.updateCacheInfo("path", "key2", updatedAttrs);
		JsonElement retrievedUpdated = cache.getAttributes("path", "key2");
		assertTrue(retrievedUpdated.getAsJsonObject().has("updated"));
		assertEquals("updated-value", retrievedUpdated.getAsJsonObject().get("updated").getAsString());
		
		// Test initializeNonemptyCache
		cache.initializeNonemptyCache("newPath", "newKey");
		assertTrue(cache.exists("newPath", null));
	}

	@Test
	public void testChildManagement() {

		final DummyBackingStorage backingStorage = new DummyBackingStorage();
		final N5JsonCache cache = new N5JsonCache( backingStorage );

		// Initialize parent and children
		cache.exists("parent", null);
		cache.list("parent");

		// Test addChild
		cache.addChild( "parent", "child1" );
		String[] children = cache.list( "parent" );
		assertTrue( Arrays.asList( children ).contains( "child1" ) );

		// Note: addChildIfPresent doesn't check or create the parent,
		// it only adds to existing cache entries

		// Test addChildIfPresent on non-cached parent
		// This should not throw and should not create the parent
		cache.addChildIfPresent("nonexistent", "child");
		children = cache.list("nonexistent");
		assertFalse(Arrays.asList(children).contains("child"));

		// Test addChildIfPresent on cached parent without children list
		cache.exists("parent2", null);
		children = cache.list("parent2"); // create children array
		cache.addChildIfPresent("parent2", "child");
		children = cache.list("parent2");
		assertTrue(Arrays.asList(children).contains("child"));
	}

	@Test
	public void testRemoveCacheHierarchy() {
		final DummyBackingStorage backingStorage = new DummyBackingStorage();
		final N5JsonCache cache = new N5JsonCache(backingStorage);
		
		// Setup hierarchy
		cache.exists("root", null);
		cache.exists("root/child1", null);
		cache.exists("root/child1/grandchild", null);
		cache.exists("root/child2", null);
		
		// Add children relationships
		cache.list("root");
		cache.addChild("root", "child1");
		cache.addChild("root", "child2");
		
		// Remove child1 and its descendants
		cache.removeCache("root", "root/child1");
		
		// Verify removal - paths should not exist anymore
		assertFalse(cache.exists("root/child1", null));
		assertFalse(cache.exists("root/child1/grandchild", null));
		
		// Verify parent's children list updated
		String[] remaining = cache.list("root");
		assertFalse(Arrays.asList(remaining).contains("child1"));
		assertTrue(Arrays.asList(remaining).contains("child2"));
		
		// Verify child2 unaffected
		assertTrue(cache.exists("root/child2", null));
	}

	@Test(expected = N5Exception.N5IOException.class)
	public void testListNonExistentGroupThrows() {

		final DummyNonExistentBackingStorage backingStorage = new DummyNonExistentBackingStorage();
		final N5JsonCache cache = new N5JsonCache(backingStorage);
		cache.list("nonexistent");
	}

	@Test
	public void testEmptyCacheInfoBehavior() {
		final DummyNonExistentBackingStorage backingStorage = new DummyNonExistentBackingStorage();
		final N5JsonCache cache = new N5JsonCache(backingStorage);
		
		// Non-existent path should return emptyCacheInfo
		assertFalse(cache.exists("nonexistent", null));
		assertFalse(cache.isGroup("nonexistent", null));
		assertFalse(cache.isDataset("nonexistent", null));
		assertNull(cache.getAttributes("nonexistent", "key"));
	}

	@Test(expected = N5Exception.class)
	public void testEmptyJsonDeepCopyThrows() {
		N5JsonCache.emptyJson.deepCopy();
	}

	@Test
	public void testCacheStateTransitions() {
		final DummyBackingStorage backingStorage = new DummyBackingStorage();
		final N5JsonCache cache = new N5JsonCache(backingStorage);
		
		// Start with emptyCacheInfo
		cache.addNewCacheInfo("path", null, null);
		
		// Transition to a nonempty cache
		cache.initializeNonemptyCache("path", "key");
		assertTrue(cache.exists("path", null));
		
		// Update existing cache
		JsonObject attrs = new JsonObject();
		attrs.addProperty("version", "1");
		cache.setAttributes("path", "key", attrs);
		
		attrs.addProperty("version", "2");
		cache.updateCacheInfo("path", "key", attrs);
		assertEquals("2", cache.getAttributes("path", "key").getAsJsonObject().get("version").getAsString());
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

		public JsonElement getAttributesFromContainer(final String path, final String cacheKey) {
			attrCallCount++;
			final JsonObject obj = new JsonObject();
			obj.addProperty("key", "value");
			return obj;
		}

		public boolean existsFromContainer(final String path, final String cacheKey) {
			existsCallCount++;
			return true;
		}

		public boolean isGroupFromContainer(final String path) {
			isGroupCallCount++;
			return true;
		}

		public boolean isDatasetFromContainer(final String path) {
			isDatasetCallCount++;
			return true;
		}

		public String[] listFromContainer(final String path) {
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

	// Helper class for non-existent paths
	protected static class DummyNonExistentBackingStorage extends DummyBackingStorage {

		@Override
		public JsonElement getAttributesFromContainer(String key, String cacheKey) {
			attrCallCount++;
			return null;
		}

		@Override
		public boolean existsFromContainer(String path, String cacheKey) {
			existsCallCount++;
			return false;
		}
	}

}
