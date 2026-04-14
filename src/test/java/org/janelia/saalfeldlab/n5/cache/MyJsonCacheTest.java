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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Arrays;
import org.janelia.saalfeldlab.n5.MetaStoreCounters;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.TrackingMetaStore;
import org.junit.Test;

import static org.janelia.saalfeldlab.n5.MetaStoreCounters.assertEqualCounters;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MyJsonCacheTest {


	@Test
	public void cacheBackingTest() {

		final TrackingMetaStore delegate = new TrackingMetaStore(new DummyRawStore());
		final DelegateStore store = new MyJsonCache(delegate);

		final MetaStoreCounters expected = new MetaStoreCounters();


		// first time we query existence of an existing directory, the delegate is called
		assertTrue(store.store_isDirectory(N5GroupPath.of("a/b/c_exists")));
		expected.incIsDir();
		assertEqualCounters(expected, delegate.counters());

		// after that, existence of the directory should be cached
		assertTrue(store.store_isDirectory(N5GroupPath.of("a/b/c_exists")));
		assertEqualCounters(expected, delegate.counters());

		// querying parent's existence should not call the delegate, because it
		// can be inferred from existence of child
		assertTrue(store.store_isDirectory(N5GroupPath.of("a/b")));
		assertTrue(store.store_isDirectory(N5GroupPath.of("a")));
		assertTrue(store.store_isDirectory(N5GroupPath.of("")));
		assertEqualCounters(expected, delegate.counters());


		// first time we query existence of a non-existing directory, the delegate is called
		assertFalse(store.store_isDirectory(N5GroupPath.of("d/e/f")));
		expected.incIsDir();
		assertEqualCounters(expected, delegate.counters());

		// after that, non-existence of the directory should be cached
		assertFalse(store.store_isDirectory(N5GroupPath.of("d/e/f")));
		assertEqualCounters(expected, delegate.counters());

		// querying parent's existence calls the delegate, because it can not be
		// inferred from non-existence of child
		assertFalse(store.store_isDirectory(N5GroupPath.of("d/e")));
		expected.incIsDir();
		assertEqualCounters(expected, delegate.counters());

		assertFalse(store.store_isDirectory(N5GroupPath.of("d/e")));
		assertEqualCounters(expected, delegate.counters());

		// querying existence of child of non-existing parent should not call
		// the delegate, because it can be inferred
		assertFalse(store.store_isDirectory(N5GroupPath.of("d/e/g")));
		assertEqualCounters(expected, delegate.counters());

		/*
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
*/
	}

	@Test
	public void testCopyOnReadPreventsExternalModification() {
/*
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
*/
	}

	@Test
	public void testCacheManipulationMethods() {
/*
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
*/
	}

	@Test
	public void testChildManagement() {
/*
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

*/
	}

	private static class DummyRawStore implements DelegateStore {

		@Override
		public JsonElement store_readAttributesJson(
				final N5GroupPath group,
				final String filename,
				final Gson gson) throws N5IOException {
			if (filename.endsWith("_null")) {
				return null;
 			} else {
				final JsonObject obj = new JsonObject();
				obj.addProperty("key", "value");
				return obj;
			}
		}

		@Override
		public boolean store_isDirectory(final N5GroupPath group) {
			return group.path().endsWith("_exists/");
		}

		@Override
		public String[] store_listDirectories(final N5GroupPath group) throws N5IOException {
			if (group.path().endsWith("_exists/"))
				return new String[] {"list"};
			else
				throw new N5IOException("Directory does not exist");
		}

		@Override
		public void store_writeAttributesJson(
				final N5GroupPath group,
				final String filename,
				final JsonElement attributes,
				final Gson gson) throws N5IOException {
		}

		@Override
		public void store_removeAttributesJson(
				final N5GroupPath group,
				final String filename) throws N5IOException {
		}

		@Override
		public void store_createDirectories(final N5GroupPath group) throws N5IOException {
		}

		@Override
		public void store_removeDirectory(final N5GroupPath group) throws N5IOException {
		}
	}
}
