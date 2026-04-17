package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Arrays;
import java.util.List;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path.N5DirectoryPath;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verify that a {@link HierarchyStore} implementation satisfies all the
 * behavioral assumptions that {@link HierarchyCache} relies on when caching
 * results from a delegate container. Any implementation of {@link
 * HierarchyStore} must satisfy these to be cacheable,
 * <p>
 * To test a specific implementation, subclass this test and implement
 * {@link #createStore()}.
 */
public abstract class AbstractHierarchyCacheContractTest {

	protected static final Gson GSON = new Gson();

	protected static final String FILENAME = "attributes.json";

	protected HierarchyStore store;

	/**
	 * Create and return a fresh, empty {@link HierarchyStore} instance.
	 * Called before each test.
	 */
	protected abstract HierarchyStore createStore();

	@Before
	public void setUp() {
		store = createStore();
	}

	// ------------------------------------------------------------------------
	// Helpers
	// ------------------------------------------------------------------------

	private static N5DirectoryPath path(final String p) {
		return N5DirectoryPath.of(p);
	}

	private static JsonElement someJson() {
		final JsonObject obj = new JsonObject();
		obj.addProperty("key", "value");
		return obj;
	}

	private static JsonElement otherJson() {
		final JsonObject obj = new JsonObject();
		obj.addProperty("key", "other");
		return obj;
	}

	// ------------------------------------------------------------------------
	// 1. store_createDirectories
	// ------------------------------------------------------------------------

	/**
	 * Assumption 1: After creating a directory, that directory exists.
	 * <p>
	 * {@code store_createDirectories("a/b")} must result in
	 * {@code store_isDirectory("a/b") == true}.
	 */
	@Test
	public void testCreateDirectories_directoryExists() {
		store.createDirectories(path("a/b"));
		assertTrue(store.isDirectory(path("a/b")));
	}

	/**
	 * Assumption 2: After creating a directory, all ancestor directories exist.
	 * <p>
	 * {@code store_createDirectories("a/b/c")} must result in
	 * {@code store_isDirectory("a") == true} and
	 * {@code store_isDirectory("a/b") == true}.
	 * <p>
	 * This is required because {@link HierarchyCache.CacheInfoDirectory#setExists()}
	 * propagates existence upward through the parent chain.
	 */
	@Test
	public void testCreateDirectories_ancestorsExist() {
		store.createDirectories(path("a/b/c"));
		assertTrue(store.isDirectory(path("a")));
		assertTrue(store.isDirectory(path("a/b")));
	}

	/**
	 * Assumption 3: Creating a directory that already exists is a no-op and
	 * does not throw.
	 */
	@Test
	public void testCreateDirectories_idempotent() {
		store.createDirectories(path("a/b"));
		store.createDirectories(path("a/b")); // must not throw
		assertTrue(store.isDirectory(path("a/b")));
	}

	// ------------------------------------------------------------------------
	// 2. store_writeAttributesJson
	// ------------------------------------------------------------------------

	/**
	 * Assumption 4: After writing a JSON attributes file, reading it back
	 * returns the written content.
	 */
	@Test
	public void testWriteAttributesJson_readBack() {
		final JsonElement json = someJson();
		store.writeAttributesJson(path("a/b"), FILENAME, json, GSON);
		assertEquals(json, store.readAttributesJson(path("a/b"), FILENAME, GSON));
	}

	/**
	 * Assumption 5: After writing a JSON attributes file, the parent directory
	 * exists.
	 * <p>
	 * {@code store_writeAttributesJson("a/b", f, ...)} must result in
	 * {@code store_isDirectory("a/b") == true}.
	 * <p>
	 * This is required because {@link HierarchyCache} calls
	 * {@code parent.setExists()} when a JSON file is successfully written.
	 */
	@Test
	public void testWriteAttributesJson_parentDirectoryExists() {
		store.writeAttributesJson(path("a/b"), FILENAME, someJson(), GSON);
		assertTrue(store.isDirectory(path("a/b")));
	}

	/**
	 * Assumption 6: After writing a JSON attributes file, all ancestor
	 * directories of its parent exist.
	 * <p>
	 * {@code store_writeAttributesJson("a/b/c", f, ...)} must result in
	 * {@code store_isDirectory("a") == true} and
	 * {@code store_isDirectory("a/b") == true}.
	 * <p>
	 * This is required because {@link HierarchyCache} recursively propagates
	 * existence upward via {@code setExists()}.
	 */
	@Test
	public void testWriteAttributesJson_ancestorDirectoriesExist() {
		store.writeAttributesJson(path("a/b/c"), FILENAME, someJson(), GSON);
		assertTrue(store.isDirectory(path("a")));
		assertTrue(store.isDirectory(path("a/b")));
	}

	/**
	 * Assumption 7: Writing a JSON attributes file a second time overwrites
	 * the first, and reading it back returns the new content.
	 */
	@Test
	public void testWriteAttributesJson_overwrite() {
		store.writeAttributesJson(path("a/b"), FILENAME, someJson(), GSON);
		store.writeAttributesJson(path("a/b"), FILENAME, otherJson(), GSON);
		assertEquals(otherJson(), store.readAttributesJson(path("a/b"), FILENAME, GSON));
	}


	// ------------------------------------------------------------------------
	// 3. store_removeDirectory
	// ------------------------------------------------------------------------

	/**
	 * Assumption 8: After removing a directory, that directory no longer
	 * exists.
	 */
	@Test
	public void testRemoveDirectory_directoryGone() {
		store.createDirectories(path("a/b"));
		store.removeDirectory(path("a/b"));
		assertFalse(store.isDirectory(path("a/b")));
	}

	/**
	 * Assumption 9: After removing a directory, all descendant directories
	 * no longer exist.
	 * <p>
	 * This is required because {@link HierarchyCache.CacheInfo#markRemoved()}
	 * propagates non-existence downward to all known children.
	 */
	@Test
	public void testRemoveDirectory_descendantsGone() {
		store.createDirectories(path("a/b/c/d"));
		store.removeDirectory(path("a/b"));
		assertFalse(store.isDirectory(path("a/b/c")));
		assertFalse(store.isDirectory(path("a/b/c/d")));
	}

	/**
	 * Assumption 10: After removing a directory, JSON attributes files in any
	 * descendant directory return {@code null} when read.
	 */
	@Test
	public void testRemoveDirectory_descendantAttributesGone() {
		store.writeAttributesJson(path("a/b/c"), FILENAME, someJson(), GSON);
		store.removeDirectory(path("a/b"));
		assertNull(store.readAttributesJson(path("a/b/c"), FILENAME, GSON));
	}

	/**
	 * Assumption 11: After removing a directory, that directory is no longer
	 * listed as a child of its parent.
	 */
	@Test
	public void testRemoveDirectory_removedFromParentListing() {
		store.createDirectories(path("a/b"));
		store.removeDirectory(path("a/b"));
		final String[] children = store.listDirectories(path("a"));
		assertFalse(Arrays.asList(children).contains("b"));
	}

	/**
	 * Assumption 12: Removing a directory that does not exist does not throw.
	 */
	@Test
	public void testRemoveDirectory_nonExistentIsNoOp() {
		store.removeDirectory(path("a/b")); // must not throw
	}

	// ------------------------------------------------------------------------
	// 4. store_isDirectory
	// ------------------------------------------------------------------------

	/**
	 * Assumption 13: A directory that has never been created does not exist.
	 */
	@Test
	public void testIsDirectory_nonExistentReturnsFalse() {
		assertFalse(store.isDirectory(path("a/b")));
	}

	/**
	 * Assumption 14: If a directory does not exist, none of its descendants
	 * exist either.
	 * <p>
	 * This is required because {@link HierarchyCache} propagates non-existence
	 * downward: if the cache knows a directory doesn't exist, it infers that
	 * all children don't exist without going to the delegate.
	 */
	@Test
	public void testIsDirectory_nonExistenceImpliesDescendantsAbsent() {
		assertFalse(store.isDirectory(path("a")));
		assertFalse(store.isDirectory(path("a/b")));
		assertFalse(store.isDirectory(path("a/b/c")));
	}

	/**
	 * Assumption 15: If a directory does not exist, reading any JSON
	 * attributes file underneath it returns {@code null}.
	 * <p>
	 * This is required because {@link HierarchyCache} propagates non-existence
	 * of a directory downward to attribute files it contains.
	 */
	@Test
	public void testIsDirectory_nonExistenceImpliesAttributesAbsent() {
		assertFalse(store.isDirectory(path("a/b")));
		assertNull(store.readAttributesJson(path("a/b"), FILENAME, GSON));
	}

	// ------------------------------------------------------------------------
	// 5. store_listDirectories
	// ------------------------------------------------------------------------

	/**
	 * Assumption 16: Listing a directory that does not exist throws
	 * {@link N5NoSuchKeyException}.
	 */
	@Test(expected = N5NoSuchKeyException.class)
	public void testListDirectories_nonExistentThrows() {
		store.listDirectories(path("a/b"));
	}

	/**
	 * Assumption 17: After creating a child directory, it appears in the
	 * listing of its parent.
	 */
	@Test
	public void testListDirectories_createdChildAppears() {
		store.createDirectories(path("a/b"));
		final List<String> children = Arrays.asList(store.listDirectories(path("a")));
		assertTrue(children.contains("b"));
	}

	/**
	 * Assumption 18: After writing a JSON file into a child directory, that
	 * child appears in the listing of its parent.
	 * <p>
	 * This is required because writing a JSON file implies the parent
	 * directory exists and is therefore listable as a child of its own parent.
	 */
	@Test
	public void testListDirectories_writtenFileImpliesChildInListing() {
		store.writeAttributesJson(path("a/b"), FILENAME, someJson(), GSON);
		final List<String> children = Arrays.asList(store.listDirectories(path("a")));
		assertTrue(children.contains("b"));
	}

	/**
	 * Assumption 19: Listing returns only immediate children, not deeper
	 * descendants.
	 */
	@Test
	public void testListDirectories_onlyImmediateChildren() {
		store.createDirectories(path("a/b/c"));
		final List<String> children = Arrays.asList(store.listDirectories(path("a")));
		assertTrue(children.contains("b"));
		assertFalse(children.contains("c"));
		assertFalse(children.contains("b/c"));
	}

	/**
	 * Assumption 20: After removing a child directory, it no longer appears
	 * in the listing of its parent.
	 */
	@Test
	public void testListDirectories_removedChildDisappears() {
		store.createDirectories(path("a/b"));
		store.createDirectories(path("a/c"));
		store.removeDirectory(path("a/b"));
		final List<String> children = Arrays.asList(store.listDirectories(path("a")));
		assertFalse(children.contains("b"));
		assertTrue(children.contains("c")); // sibling unaffected
	}

	// ------------------------------------------------------------------------
	// 6. store_readAttributesJson
	// ------------------------------------------------------------------------

	/**
	 * Assumption 21: Reading a JSON attributes file that does not exist
	 * returns {@code null} (does not throw).
	 */
	@Test
	public void testReadAttributesJson_nonExistentReturnsNull() {
		store.createDirectories(path("a/b"));
		assertNull(store.readAttributesJson(path("a/b"), FILENAME, GSON));
	}

	/**
	 * Assumption 22: Reading a JSON attributes file is stable: repeated reads
	 * without intervening writes return the same content.
	 */
	@Test
	public void testReadAttributesJson_stable() {
		final JsonElement json = someJson();
		store.writeAttributesJson(path("a/b"), FILENAME, json, GSON);
		final JsonElement first = store.readAttributesJson(path("a/b"), FILENAME, GSON);
		final JsonElement second = store.readAttributesJson(path("a/b"), FILENAME, GSON);
		assertEquals(first, second);
	}
}