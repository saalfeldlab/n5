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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.janelia.saalfeldlab.n5.MetaStoreCounters;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;
import org.janelia.saalfeldlab.n5.TrackingMetaStore;
import org.junit.Test;

import static org.janelia.saalfeldlab.n5.MetaStoreCounters.assertEqualCounters;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MyJsonCacheTest {

	private static Set<String> setOf(String... values) {
		return Stream.of(values).collect(Collectors.toSet());
	}

	@Test
	public void cacheBackingTest() {

		final TrackingMetaStore delegate = new TrackingMetaStore(new DummyRawStore());
		final DelegateStore store = new MyJsonCache(delegate);
		final MetaStoreCounters expected = new MetaStoreCounters();


		// ----------------------------
		//  isDirectory
		// ----------------------------

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

		// querying existence of child of non-existing parent should not call
		// the delegate, because it can be inferred
		assertFalse(store.store_isDirectory(N5GroupPath.of("d/e/g")));
		assertEqualCounters(expected, delegate.counters());



		// ----------------------------
		//  list
		// ----------------------------

		// listing a non-existing directory should throw N5IOException, for both cached and non-cached paths.
		// if we have non-existence cached, list should not call the delegate
		assertThrows(N5IOException.class, () -> store.store_listDirectories(N5GroupPath.of("d/e/f")));
		assertEqualCounters(expected, delegate.counters());

		// for a non-cached path, list calls the delegate
		assertThrows(N5IOException.class, () -> store.store_listDirectories(N5GroupPath.of("c")));
		expected.incList();
		assertEqualCounters(expected, delegate.counters());

		// now, the path should be cached
		assertThrows(N5IOException.class, () -> store.store_listDirectories(N5GroupPath.of("c")));
		assertEqualCounters(expected, delegate.counters());



		// listing an existing directory calls the delegate once and caches the result
		assertEquals(setOf("list"), setOf(store.store_listDirectories(N5GroupPath.of("a_exists"))));
		expected.incList();
		assertEqualCounters(expected, delegate.counters());

		// now, the list should be cached
		assertEquals(setOf("list"), setOf(store.store_listDirectories(N5GroupPath.of("a_exists"))));
		assertEqualCounters(expected, delegate.counters());

		// creating children under a directory with a cached listing should modify the cached listing and not list again from the delegate
		store.store_createDirectories(N5GroupPath.of("a_exists/b/c"));
		expected.incMkDir();
		assertEquals(setOf("list", "b"), setOf(store.store_listDirectories(N5GroupPath.of("a_exists"))));
		assertEqualCounters(expected, delegate.counters());

		// removing children under a directory with a cached listing should modify the cached listing and not list again from the delegate
		store.store_removeDirectory(N5GroupPath.of("a_exists/list"));
		expected.incRmDir();
		assertEquals(setOf("b"), setOf(store.store_listDirectories(N5GroupPath.of("a_exists"))));
		assertEqualCounters(expected, delegate.counters());



		// ----------------------------
		// readAttributesJson
		// ----------------------------

		final Gson gson = new Gson();

		// reading an existing attributes file from the delegate caches its content
		final JsonElement attr1 = store.store_readAttributesJson(N5GroupPath.of("h/i"), "key", gson);
		expected.incReadAttr();
		assertEqualCounters(expected, delegate.counters());

		// no call to delegate when we read the attribute file again
		final JsonElement attr2 = store.store_readAttributesJson(N5GroupPath.of("h/i"), "key", gson);
		assertEquals(attr1, attr2);
		assertEqualCounters(expected, delegate.counters());

		// existence of parent directories should have been inferred
		assertTrue(store.store_isDirectory(N5GroupPath.of("h/i")));
		assertTrue(store.store_isDirectory(N5GroupPath.of("h")));
		assertEqualCounters(expected, delegate.counters());


		// reading a non-existing attributes file should cache its non-existence
		assertNull(store.store_readAttributesJson(N5GroupPath.of("h/i"), "key_null", gson));
		expected.incReadAttr();
		assertEqualCounters(expected, delegate.counters());

		// no call to delegate when we try to read the attribute file again
		assertNull(store.store_readAttributesJson(N5GroupPath.of("h/i"), "key_null", gson));
		assertEqualCounters(expected, delegate.counters());



		// reading an attributes file in a directory that is known to not exist, should not attempt to read from the delegate
		assertNull(store.store_readAttributesJson(N5GroupPath.of("d/e/f"), "key", gson));
		assertEqualCounters(expected, delegate.counters());



		// ----------------------------
		// writeAttributesJson
		// ----------------------------

		// writing an attributes file calls the delegate
		store.store_writeAttributesJson(N5GroupPath.of("d/e/f"), "key", attr1, gson);
		expected.incWriteAttr();
		assertEqualCounters(expected, delegate.counters());

		// existence of parent directories should be inferred
		assertTrue(store.store_isDirectory(N5GroupPath.of("d/e/f")));
		assertTrue(store.store_isDirectory(N5GroupPath.of("d/e")));
		assertTrue(store.store_isDirectory(N5GroupPath.of("d")));
		assertEqualCounters(expected, delegate.counters());

		// reading the attributes file should not call the delegate
		assertEquals(attr1, store.store_readAttributesJson(N5GroupPath.of("d/e/f"), "key", gson));
		assertEqualCounters(expected, delegate.counters());

		// overwriting with identical attributes should not call delegate
		store.store_writeAttributesJson(N5GroupPath.of("d/e/f"), "key", attr2, gson);
		assertEqualCounters(expected, delegate.counters());

		// overwriting with modified attributes should call the delegate
		attr1.getAsJsonObject().addProperty("modified", "value");
		store.store_writeAttributesJson(N5GroupPath.of("d/e/f"), "key", attr1, gson);
		expected.incWriteAttr();
		assertEqualCounters(expected, delegate.counters());



		// ----------------------------
		// removeDirectory
		// ----------------------------

		// removing a directory calls the delegate
		store.store_removeDirectory(N5GroupPath.of("d"));
		expected.incRmDir();
		assertEqualCounters(expected, delegate.counters());

		// nested files and directories should be inferred to be removed as well
		assertFalse(store.store_isDirectory(N5GroupPath.of("d/e/f")));
		assertFalse(store.store_isDirectory(N5GroupPath.of("d/e")));
		assertFalse(store.store_isDirectory(N5GroupPath.of("d")));
		assertNull(store.store_readAttributesJson(N5GroupPath.of("d/e/f"), "key", gson));
		assertEqualCounters(expected, delegate.counters());

		// removing a directory that is known to not exist should not call the delegate
		store.store_removeDirectory(N5GroupPath.of("d"));
		assertEqualCounters(expected, delegate.counters());



		// ----------------------------
		// createDirectories
		// ----------------------------

		// creating a new directory should call the delegate
		store.store_createDirectories(N5GroupPath.of("j/k/l"));
		expected.incMkDir();
		assertEqualCounters(expected, delegate.counters());

		// creating a (known to be) existing directory again should not call the delegate
		store.store_createDirectories(N5GroupPath.of("j/k/l"));
		store.store_createDirectories(N5GroupPath.of("a/b"));
		assertEqualCounters(expected, delegate.counters());

	}

	@Test
	public void testCopyOnReadPreventsExternalModification() {

		final TrackingMetaStore delegate = new TrackingMetaStore(new DummyRawStore());
		final DelegateStore store = new MyJsonCache(delegate);
		final Gson gson = new Gson();

		// Get attributes and modify the returned object
		JsonElement attrs1 = store.store_readAttributesJson(N5GroupPath.of("path"), "key", gson);
		attrs1.getAsJsonObject().addProperty("modified", "value");

		// Get attributes again - should not contain the modification
		JsonElement attrs2 = store.store_readAttributesJson(N5GroupPath.of("path"), "key", gson);
		assertFalse(attrs2.getAsJsonObject().has("modified"));

		// Verify both calls return different instances
		assertNotSame(attrs1, attrs2);
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
				throw new N5NoSuchKeyException("Directory does not exist");
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
