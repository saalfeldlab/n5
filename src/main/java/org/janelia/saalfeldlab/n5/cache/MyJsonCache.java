package org.janelia.saalfeldlab.n5.cache;

import com.google.gson.JsonElement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.N5Path;
import org.janelia.saalfeldlab.n5.N5Path.N5FilePath;
import org.janelia.saalfeldlab.n5.N5Path.N5GroupPath;

public class MyJsonCache {

	private final MyJsonCacheableContainer container;

	/**
	 * Maps N5Path to MyCacheInfo
	 */
	private final ConcurrentHashMap<String, MyCacheInfo> infos;
	private final MyCacheInfo root;

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


	public MyJsonCache(final MyJsonCacheableContainer container) {
		this.container = container;

		infos = new ConcurrentHashMap<>();

		// create root CacheInfo (we assume it exists)
		root = new CacheInfoDirectory(N5GroupPath.of(""), null);
		root.valid = true;
		add(root);
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
	public JsonElement getAttributes(final N5GroupPath group, final String attributesKey) throws N5IOException {

		final N5FilePath path = group.resolve(attributesKey).asFile();
		final CacheInfoAttributes info = getOrCreate(path);
		synchronized (info) {
			if (!info.valid) {
				// TODO: Reconsider validation. Alternatives would be to
				//       do it all externally (done here), OR
				//       do it all internally in a synchronized method taking container
				//       OR maybe something else entirely...
				info.json = container.my_getAttributesFromContainer(group, attributesKey);
				info.exists = (info.json != null); // TODO: remove exists field, and replace with exists() method?
				info.valid = true;
			}
		}
		return info.json;
	}

	public void setAttributes(final N5GroupPath group, final String attributesKey, final JsonElement attributes) throws N5IOException {

		final N5FilePath path = group.resolve(attributesKey).asFile();
		final CacheInfoAttributes info = getOrCreate(path);
		synchronized (info) {
			if (!info.valid) {
				throw new IllegalStateException("Unexpected invalid CacheInfoAttributes " + this);
			}
			info.json = attributes;
			info.exists = true;
		}
	}

	public void remove(final N5GroupPath group) {

		final CacheInfoDirectory info = getOrCreate(group);
		info.markRemoved();
		final CacheInfoDirectory parent = info.parent;
		if (parent != null) {
			synchronized (parent) {
				if (parent.list != null) {
					final String[] pathParts = group.components();
					parent.list.remove(pathParts[pathParts.length - 1]);
				}
			}
		}
	}

}
