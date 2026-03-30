package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Exception.N5NoSuchKeyException;
import org.janelia.saalfeldlab.n5.N5Path;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;

public class MyJsonCache implements DelegateStore {

	// ------------------------------------------------------------------------
	//
	// MyCacheInfo
	//
	// ------------------------------------------------------------------------

	/*
	 * Implementation notes:
	 *
	 * Synchronization:
	 *   Within in a synchronized block on a CacheInfo, it is ok to take
	 *   additional locks on parents but not on children.
	 */

	static abstract class MyCacheInfo {

		protected final CacheInfoDirectory parent;

		// When we add a new CacheInfo, we put it into the map immediately, to
		// have something ot synchronize on. However, we are still initializing
		// it (for example reading attributes file from container). When
		// initialization is done, we set valid:=true.
		protected boolean valid = false;

		MyCacheInfo(final CacheInfoDirectory parent) {
			this.parent = parent;
			if (parent != null)
				parent.addChild(this);
		}

		abstract N5Path path();

		abstract void markRemoved();

		boolean valid() {
			return valid;
		}

		CacheInfoDirectory parent() {
			return parent;
		}

		// if this entry is valid: does the path exist
		abstract boolean exists();

		synchronized boolean isKnownToExist() {
			return valid() && exists();
		}

		abstract boolean isKnownToNotExist();

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

		@Override
		boolean exists() {
			return json != null;
		}

		@Override
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
			json = null;
		}

		synchronized void setJson(final JsonElement json) {
			if (!valid && json != null)
				parent.setExists();
			this.json = json;
			valid = true;
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

		@Override
		boolean exists() {
			return exists;
		}

		@Override
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
		 * The directory represented by {@code info} was just created, re-created,
		 * or otherwise observed to exist.
		 * <p>
		 * Make sure that its {@code valid} and {@code exists} flags are set, and it
		 * is present in its parent's {@code children} list (if that exists).
		 * <p>
		 * Recursively call for parent, because we know that all parent directories
		 * must exist now as well.
		 */
		void setExists() {

			boolean addToParent = false;
			synchronized (this) {
				if(!isKnownToExist()) {
					addToParent = true;
					valid = true;
					exists = true;
				}
			}
			if (addToParent) {
				// This group was just created or re-created.
				// We need to add it to the parent's list, if that exists.
				// Also, we want to recursively set exists=true for parents.
				if (parent != null) {
					parent.addToList(path.filename());
					parent.setExists();
				}
			}
		}

	}


	// ------------------------------------------------------------------------
	//
	// MyJsonCache
	//
	// ------------------------------------------------------------------------

	private final DelegateStore container;
	private final Gson gson;

	/**
	 * Maps N5Path to MyCacheInfo
	 */
	private final ConcurrentHashMap<String, MyCacheInfo> infos;
	private final MyCacheInfo root;

	public MyJsonCache(
			final DelegateStore container,
			final Gson gson
	) {
		this.container = container;
		this.gson = gson;

		infos = new ConcurrentHashMap<>();

		// create root CacheInfo (we assume it exists)
		root = new CacheInfoDirectory(N5GroupPath.of(""), null);
		infos.put(root.path().normalPath(), root);
	}

	private CacheInfoDirectory getOrCreate(N5GroupPath path) {
		final MyCacheInfo info = infos.get(path.normalPath());
		if (info != null)
			return info.asDirectory();
		final CacheInfoDirectory parent = getOrCreate(path.parent());
		return infos.computeIfAbsent(path.normalPath(), p -> new CacheInfoDirectory(path, parent)).asDirectory();
	}

	private CacheInfoAttributes getOrCreate(N5FilePath path) {
		final MyCacheInfo info = infos.get(path.normalPath());
		if (info != null)
			return info.asAttributes();
		final CacheInfoDirectory parent = getOrCreate(path.parent());
		return infos.computeIfAbsent(path.normalPath(), p -> new CacheInfoAttributes(path, parent)).asAttributes();
	}




	// TODO: inline and remove?
	private Gson getGson() {
		return gson;
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
	public JsonElement store_readAttributesJson(final N5GroupPath group, final String attributesKey) throws N5IOException {

		final N5FilePath path = group.resolve(attributesKey).asFile();
		final CacheInfoAttributes info = getOrCreate(path);
		synchronized (info) {
			if (!info.valid() && !info.isKnownToNotExist()) {
				info.setJson(container.store_readAttributesJson(group, attributesKey));
			}
			return info.json;
		}
	}

	@Override
	public void store_writeAttributesJson(final N5GroupPath group, final String filename, final JsonElement attributes) throws N5IOException {

		container.store_writeAttributesJson(group, filename, attributes);
		/*
		 * Gson only filters out nulls when you write the JsonElement. This
		 * means it doesn't filter them out when caching.
		 * To handle this, we explicitly writer the existing JsonElement to
		 * a new JsonElement.
		 * The output is identical to the input if:
		 * - serializeNulls is true
		 * - no null values are present
		 */
		final JsonElement json = getGson().serializeNulls() ? attributes : getGson().toJsonTree(attributes);
		final N5FilePath path = group.resolve(filename).asFile();
		final CacheInfoAttributes info = getOrCreate(path);
		info.setJson(json);
	}

	@Override
	public boolean store_isDirectory(final N5GroupPath group) {

		final CacheInfoDirectory info = getOrCreate(group);
		synchronized (info) {
			if (!info.valid) {
				info.exists = container.store_isDirectory(group);
				info.valid = true;
				// TODO: Can isDirectoryFromContainer throw a N5IOException?
			}
		}
		return info.exists;
	}

	@Override
	public String[] store_listDirectories(final N5GroupPath group) throws N5IOException {

		final CacheInfoDirectory info = getOrCreate(group);
		boolean setExists = false;
		synchronized (info) {
			if (!info.valid || (info.exists && info.list == null)) {
				try {
					final String[] list = container.store_listDirectories(group);
					info.list = new ArrayList<>(List.of(list));
					setExists = true;
				} catch (N5NoSuchKeyException e) {
					info.exists = false;
					info.valid = true;
					throw new N5NoSuchKeyException(e);
				}
				// NB: If listFromContainer throw a N5IOException other than
				// N5NoSuchKeyException, we do not catch it and just let it be
				// thrown. info.valid==false remains, and we will retry next
				// time...
			}
		}
		if (setExists) {
			info.setExists();
		}
		if (!info.exists) {
			throw new N5NoSuchKeyException("No such file: " + group);
		}
		return info.list.toArray(new String[0]);
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
