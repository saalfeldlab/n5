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

	static abstract class MyCacheInfo {

		protected final CacheInfoDirectory parent;
		protected boolean exists = true;

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

		abstract N5Path getPath();

		abstract void markRemoved();

		CacheInfoAttributes asAttributes() {
			throw new IllegalStateException("Not a CacheInfoAttributes: " + this);
		}

		CacheInfoDirectory asDirectory() {
			throw new IllegalStateException("Not a CacheInfoDirectory: " + this);
		}
	}


	static class CacheInfoAttributes extends MyCacheInfo {

		protected final N5FilePath path;
		protected JsonElement json;

		CacheInfoAttributes(final N5FilePath path, CacheInfoDirectory parent) {
			super(parent);
			this.path = path;
		}

		@Override
		N5Path getPath() {
			return path;
		}

		@Override
		CacheInfoAttributes asAttributes() {
			return this;
		}

		@Override
		synchronized void markRemoved() {
			json = null;
			exists = false;
		}
	}


	static class CacheInfoDirectory extends MyCacheInfo {

		protected final N5GroupPath path;
		protected final List<MyCacheInfo> children = new ArrayList<>();
		protected List<String> list;

		CacheInfoDirectory(final N5GroupPath path, CacheInfoDirectory parent) {
			super(parent);
			this.path = path;
		}

		@Override
		N5Path getPath() {
			return path;
		}

		@Override
		CacheInfoDirectory asDirectory() {
			return this;
		}

		void addChild(final MyCacheInfo child) {
			synchronized (children) {
				children.add(child);
			}
		}

		@Override
		synchronized void markRemoved() {
			list = null;
			exists = false;
			children.forEach(MyCacheInfo::markRemoved);
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
		add(root);
	}

	// TODO: inline and remove?
	private Gson getGson() {
		return gson;
	}



	private void add(MyCacheInfo info) {
		infos.put(info.getPath().normalPath(), info);
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
			if (!info.valid) {
				// TODO: Reconsider validation. Alternatives would be to
				//       do it all externally (done here), OR
				//       do it all internally in a synchronized method taking container
				//       OR maybe something else entirely...
				info.json = container.store_readAttributesJson(group, attributesKey);
				info.exists = (info.json != null); // TODO: remove exists field, and replace with exists() method?
				info.valid = true;
			}
		}
		return info.json;
	}

	@Override
	public void store_writeAttributesJson(final N5GroupPath group, final String filename, final JsonElement attributes) throws N5IOException {

		final N5FilePath path = group.resolve(filename).asFile();
		final CacheInfoAttributes info = getOrCreate(path);
		synchronized (info) {
			if (!info.valid) {
				throw new IllegalStateException("Unexpected invalid CacheInfoAttributes " + this);
			}
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
			info.json = getGson().serializeNulls() ? attributes : getGson().toJsonTree(attributes);
			info.exists = true;
		}
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
		synchronized (info) {
			if (!info.valid || (info.exists && info.list == null)) {
				try {
					final String[] list = container.store_listDirectories(group);
					info.list = new ArrayList<>(List.of(list));
					info.exists = true;
					info.valid = true;
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
		if (!info.exists) {
			throw new N5NoSuchKeyException("No such file: " + group);
		}
		return info.list.toArray(new String[0]);
	}

	@Override
	public void store_createDirectories(final N5GroupPath group) throws N5IOException {

		final CacheInfoDirectory info = getOrCreate(group);
		boolean addToParent = false;
		synchronized (info) {
			if(!info.valid || !info.exists) {
				container.store_createDirectories(group);
				addToParent = true;
				info.valid = true;
				info.exists = true;
			}
		}
		// This group was just created or re-created.
		// We need to add it to the parent's list, if that exists.
		// Also, we want to recursively set exists=true for parents.
		if (addToParent) {
			final CacheInfoDirectory parent = info.parent;
			if (parent != null) {
				synchronized (parent) {
					if (parent.list != null) {
						parent.list.add(info.path.filename());
					}
				}
				setExists(parent);
			}
		}
	}

	@Override
	public void store_removeDirectory(final N5GroupPath group) throws N5IOException {

		container.store_removeDirectory(group);

		final CacheInfoDirectory info = getOrCreate(group);
		info.markRemoved();
		final CacheInfoDirectory parent = info.parent;
		if (parent != null) {
			synchronized (parent) {
				if (parent.list != null) {
					parent.list.remove(group.filename());
				}
			}
		}
	}

	/**
	 * The directory represented by {@code info} was just created or re-created.
	 * Make sure that its {@code valid} and {@code exists} flags are set, and it
	 * is present in its parent's {@code children} list (if that exists).
	 * <p>
	 * Recursively call for parent, because we know that all parent directories
	 * must exist now as well.
	 */
	private void setExists(final CacheInfoDirectory info) {
		boolean addToParent = false;
		synchronized (info) {
			if(!info.valid || !info.exists) {
				addToParent = true;
				info.valid = true;
				info.exists = true;
			}
		}
		// This group was just created or re-created.
		// We need to add it to the parent's list, if that exists.
		// Also, we want to recursively set exists=true for parents.
		if (addToParent) {
			final CacheInfoDirectory parent = info.parent;
			if (parent != null) {
				synchronized (parent) {
					if (parent.list != null) {
						parent.list.add(info.path.filename());
					}
				}
				setExists(parent);
			}
		}
	}

}
