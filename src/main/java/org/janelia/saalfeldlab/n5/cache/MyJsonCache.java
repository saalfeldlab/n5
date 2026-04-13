package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;

public class MyJsonCache implements DelegateStore {

	/*
	 * Implementation notes:
	 *
	 * Within in a synchronized block on a CacheInfo, it is ok to take
	 * additional locks on parents but not on children.
	 *
	 * When we add a new CacheInfo, we add it into the JsonCache's
	 * ConcurrentHashMap immediately, to have something to synchronize on.
	 * However, we are still initializing the CacheInfo (for example reading
	 * attributes file from container). When initialization is done, we set
	 * info.valid=true.
	 */

	// ------------------------------------------------------------------------
	//
	// MyCacheInfo
	//
	// ------------------------------------------------------------------------

	static abstract class MyCacheInfo {

		protected final CacheInfoDirectory parent;

		protected boolean valid = false;

		MyCacheInfo(final CacheInfoDirectory parent) {
			this.parent = parent;
			if (parent != null)
				parent.addChild(this);
		}

		abstract N5Path path();

		/**
		 * When deleting a directory (and its contents, recursively) this method
		 * is called to mark this {@code CacheInfo} and all its children as
		 * "known to not exist". Also remove the directory from the listing of
		 * its parent (if that exists).
		 */
		abstract void markRemoved();

		boolean valid() {
			return valid;
		}

		CacheInfoAttributes asAttributes() {
			throw new IllegalStateException("Not a CacheInfoAttributes: " + this);
		}

		CacheInfoDirectory asDirectory() {
			throw new IllegalStateException("Not a CacheInfoDirectory: " + this);
		}
	}


	/**
	 * A cache entry referring to an attributes json file.
	 */
	static class CacheInfoAttributes extends MyCacheInfo {

		private final N5FilePath path;
		private JsonElement json;

		CacheInfoAttributes(final N5FilePath path, CacheInfoDirectory parent) {
			super(parent);
			this.path = path;
		}

		@Override
		CacheInfoAttributes asAttributes() {
			return this;
		}

		@Override
		N5Path path() {
			return path;
		}

		/**
		 * Returns {@code true} if it is known (by direct observation or
		 * implicitly) that this file <em>does not</em> exist.
		 * <p>
		 * Returns {@code false} if this file exists, or if its unknown whether
		 * it exists.
		 */
		synchronized boolean isKnownToNotExist() {
			if (!valid) {
				// if the parent doesn't exist, then this doesn't exist either
				if (parent.isKnownToNotExist()) {
					valid = true;
					json = null;
				}
			}
			return valid && json == null;
		}

		@Override
		void markRemoved() {
			valid = true;
			json = null;
		}

		synchronized void setJson(final JsonElement json) {
			if (json != null)
				parent.setExists();
			this.json = json;
			valid = true;
		}

		JsonElement json() {
			return json;
		}
	}


	/**
	 * A cache entry referring to a directory.
	 */
	static class CacheInfoDirectory extends MyCacheInfo {

		private boolean exists = true;
		private final N5GroupPath path;
		private final List<MyCacheInfo> children = new ArrayList<>();
		private List<String> list;

		CacheInfoDirectory(final N5GroupPath path, CacheInfoDirectory parent) {
			super(parent);
			this.path = path;
		}

		@Override
		CacheInfoDirectory asDirectory() {
			return this;
		}

		@Override
		N5Path path() {
			return path;
		}

		boolean exists() {
			return exists;
		}

		/**
		 * Returns {@code true} if it is known (by direct observation or
		 * implicitly) that this directory or file exists.
		 * <p>
		 * Returns {@code false} if this directory or file does not exist, or if
		 * its unknown whether it exists.
		 */
		synchronized boolean isKnownToExist() {
			return valid() && exists();
		}

		/**
		 * Returns {@code true} if it is known (by direct observation or
		 * implicitly) that this directory <em>does not</em> exist.
		 * <p>
		 * Returns {@code false} if this directory exists, or if its unknown
		 * whether it exists.
		 */
		synchronized boolean isKnownToNotExist() {
			if (!valid) {
				// if the parent doesn't exist, then this doesn't exist either
				if (parent != null && parent.isKnownToNotExist()) {
					valid = true;
					exists = false;
				}
			}
			return valid && !exists;
		}

		void addChild(final MyCacheInfo child) {
			synchronized (children) {
				children.add(child);
			}
		}

		String[] getList() {
			return list == null ? null : list.toArray(new String[0]);
		}

		synchronized void setList(final String[] list) {
			this.list = new ArrayList<>(Arrays.asList(list));
			setExists();
		}

		private synchronized void removeFromList(final String filename) {
			if (list != null) {
				list.remove(filename);
			}
		}

		private synchronized void addToList(final String filename) {
			if (list != null && !list.contains(filename)) {
				list.add(filename);
			}
		}

		@Override
		void markRemoved() {
			synchronized (this) {
				list = null;
				exists = false;
			}
			if (parent != null) {
				parent.removeFromList(path.filename());
			}
			synchronized(children) {
				children.forEach(MyCacheInfo::markRemoved);
			}
		}

		/**
		 * The directory represented by {@code info} was just observed to not exist.
		 * Set {@code valid=true} and {@code exists=false}.
		 * <p>
		 * This method is only used by {@code isDirectory} checks. It will not
		 * mark children as non-existing or remove this from the parents list.
		 */
		synchronized void setNotExists() {
			exists = false;
			valid = true;
		}

		/**
		 * The directory represented by {@code info} was just created, re-created,
		 * or otherwise observed to exist.
		 * <p>
		 * Make sure that its {@code valid} and {@code exists} flags are set, and it
		 * is present in its parent's {@code children} list (if that exists).
		 * <p>
		 * Recursively call for parent, because we know that all parent directories
		 * must exist now as well.
		 */
		synchronized void setExists() {

			if (!isKnownToExist()) {
				// This group was just created or re-created.
				// We need to add it to the parent's list, if that exists.
				// Also, we want to recursively set exists=true for parents.
				if (parent != null) {
					parent.addToList(path.filename());
					parent.setExists();
				}
				valid = true;
				exists = true;
			}
		}
	}


	// ------------------------------------------------------------------------
	//
	// MyJsonCache
	//
	// ------------------------------------------------------------------------

	private final DelegateStore container;

	/**
	 * Maps N5Path to MyCacheInfo
	 */
	private final ConcurrentHashMap<N5Path, MyCacheInfo> infos;

	public MyJsonCache(final DelegateStore container) {
		this.container = container;

		infos = new ConcurrentHashMap<>();

		// create root CacheInfo
		final N5GroupPath root = N5GroupPath.of("");
		infos.put(root, new CacheInfoDirectory(root, null));
	}

	private CacheInfoDirectory getOrCreate(N5GroupPath path) {
		final MyCacheInfo info = infos.get(path);
		if (info != null)
			return info.asDirectory();
		final CacheInfoDirectory parent = getOrCreate(path.parent());
		return infos.computeIfAbsent(path, p -> new CacheInfoDirectory(path, parent)).asDirectory();
	}

	private CacheInfoAttributes getOrCreate(N5FilePath path) {
		final MyCacheInfo info = infos.get(path);
		if (info != null)
			return info.asAttributes();
		final CacheInfoDirectory parent = getOrCreate(path.parent());
		return infos.computeIfAbsent(path, p -> new CacheInfoAttributes(path, parent)).asAttributes();
	}






	/**
	 * Returns a {@link JsonElement} containing the deserialized
	 * {@code attributesKey} file in the given {@code group}.
	 * <p>
	 * (Typically, the {@code attributesKey} is <em>attributes.json</em> for N5,
	 * and <em>.zarray</em>, <em>.zattrs</em>, or <em>.zgroup</em> for Zarr.)
	 *
	 * @param group
	 * @param attributesKey
	 *
	 * @return the attributes as a json element.
	 */
	@Override
	public JsonElement store_readAttributesJson(final N5GroupPath group, final String attributesKey, final Gson gson) throws N5IOException {

		final N5FilePath path = group.resolve(attributesKey).asFile();
		final CacheInfoAttributes info = getOrCreate(path);
		synchronized (info) {
			if (!info.valid() && !info.isKnownToNotExist()) {
				info.setJson(container.store_readAttributesJson(group, attributesKey, gson));
			}
			return info.json();
		}
	}

	@Override
	public void store_writeAttributesJson(final N5GroupPath group, final String filename, final JsonElement attributes, final Gson gson) throws N5IOException {

		container.store_writeAttributesJson(group, filename, attributes, gson);
		/*
		 * Gson only filters out nulls when you write the JsonElement. This
		 * means it doesn't filter them out when caching.
		 * To handle this, we explicitly writer the existing JsonElement to
		 * a new JsonElement.
		 * The output is identical to the input if:
		 * - serializeNulls is true
		 * - no null values are present
		 */
		final JsonElement json = gson.serializeNulls() ? attributes : gson.toJsonTree(attributes);
		final N5FilePath path = group.resolve(filename).asFile();
		final CacheInfoAttributes info = getOrCreate(path);
		info.setJson(json);
	}

	@Override
	public void store_removeAttributesJson(
			final N5GroupPath group,
			final String filename) throws N5IOException {

		final N5FilePath path = group.resolve(filename).asFile();
		final CacheInfoAttributes info = getOrCreate(path);
		synchronized (info) {
			if (!info.isKnownToNotExist()) {
				container.store_removeAttributesJson(group, filename);
				info.markRemoved();
			}
		}
	}

	@Override
	public boolean store_isDirectory(final N5GroupPath group) {

		final CacheInfoDirectory info = getOrCreate(group);
		synchronized (info) {
			if (!info.valid()) {
				final boolean exists = container.store_isDirectory(group);
				if (exists)
					info.setExists();
				else
					info.setNotExists();
			}
			return info.exists();
		}
	}

	@Override
	public String[] store_listDirectories(final N5GroupPath group) throws N5IOException {

		final CacheInfoDirectory info = getOrCreate(group);
		synchronized (info) {
			if (info.isKnownToNotExist()) {
				throw new N5NoSuchKeyException("No such file: " + group);
			}

			String[] list = info.getList();
			if (list == null) {
				try {
					list = container.store_listDirectories(group);
					info.setList(list);
				} catch (N5NoSuchKeyException e) {
					info.setNotExists();
					throw new N5NoSuchKeyException(e);
				}
				// NB: If listFromContainer throw a N5IOException other than
				// N5NoSuchKeyException, we do not catch it and just let it be
				// thrown. info.valid==false remains, and we will retry next
				// time...
			}
			return list;
		}
	}

	@Override
	public void store_removeDirectory(final N5GroupPath group) throws N5IOException {

		final CacheInfoDirectory info = getOrCreate(group);
		if (!info.isKnownToNotExist())
			container.store_removeDirectory(group);
		info.markRemoved();
	}

	@Override
	public void store_createDirectories(final N5GroupPath group) throws N5IOException {

		final CacheInfoDirectory info = getOrCreate(group);
		if (!info.isKnownToExist())
			container.store_createDirectories(group);
		info.setExists();
	}

}
