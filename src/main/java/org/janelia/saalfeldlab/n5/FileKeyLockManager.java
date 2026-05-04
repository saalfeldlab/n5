package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.janelia.saalfeldlab.n5.LockingPolicy.STRICT;

/**
 * Provides thread-safe and process-safe read/write locking for filesystem paths.
 * Uses thread locks for JVM coordination and file locks for inter-process coordination.
 */
class FileKeyLockManager {

	private static final Map<LockingPolicy, FileKeyLockManager> managers = Collections.synchronizedMap(new EnumMap<>(LockingPolicy.class));

	static FileKeyLockManager forPolicy(final LockingPolicy policy) {
		return managers.computeIfAbsent(policy, FileKeyLockManager::new);
	}

	/**
	 * @deprecated use {@link FileKeyLockManager#forPolicy(LockingPolicy)}
	 */
	@Deprecated
	static final FileKeyLockManager FILE_LOCK_MANAGER = forPolicy(STRICT);

	private final LockingPolicy policy;

	/**
	 * Create a new {@link FileKeyLockManager} with the specified locking policy.
	 * <p>
	 * The given locking {@link LockingPolicy policy} applies to OS-level locking.
	 * For both the {@code STRICT} and {@code PERMISSIVE} policy, a {@link
	 * FileLock} is obtained. If this fails, {@code STRICT} will throw an {@code
	 * IOException}. {@code PERMISSIVE} will proceed without locking. {@code
	 * UNSAFE} will not attempt OS-level locking, however will still manage
	 * mutual exclusion of readers and writers in the same JVM. Trying to lock
	 * the same path with different locking policies will throw an {@code
	 * IOException}.
	 *
	 * @param policy
	 * 		the locking policy
	 */
	private FileKeyLockManager(final LockingPolicy policy) {
		this.policy = policy;
	}

	private final ConcurrentHashMap<String, WeakValue> locks = new ConcurrentHashMap<>();

	private final ReferenceQueue<KeyLockState> refQueue = new ReferenceQueue<>();

	private static class WeakValue extends WeakReference<KeyLockState> {

		final String key;

		WeakValue(
				final String key,
				final KeyLockState value,
				final ReferenceQueue<KeyLockState> queue) {

			super(value, queue);
			this.key = key;
		}
	}

	/**
	 * Remove entries from the cache whose references have been
	 * garbage-collected.
	 */
	private void cleanUp()
	{
		while (true) {
			final WeakValue ref = (WeakValue) refQueue.poll();
			if (ref == null)
				break;
			locks.remove(ref.key, ref);
		}
	}

	private KeyLockState keyLockState(final Path path, final LockingPolicy policy) throws IOException {

		final String key = path.toAbsolutePath().toString();

		cleanUp();

		final WeakValue existingRef = locks.get(key);
		KeyLockState state = existingRef == null ? null : existingRef.get();
		if (state != null) {
			return state;
		}

		final KeyLockState newState = new KeyLockState(path, policy);
		while (state == null) {
			final WeakValue ref = locks.compute(key,
					(k, v) -> (v != null && v.get() != null)
							? v
							: new WeakValue(k, newState, refQueue));
			state = ref.get();
		}
		return state;
	}

	/**
	 * Acquires a read lock for the specified key. Multiple threads can hold
	 * read locks for the same key simultaneously.
	 * <p>
	 * The first reader will acquire a shared file lock. Subsequent readers
	 * only acquire the thread-level lock.
	 *
	 * @param path
	 * 		the key (file path) to lock for reading
	 *
	 * @return a {@link LockedChannel} that must be closed when done
	 *
	 * @throws IOException
	 * 		if acquiring the file lock fails
	 */
	public LockedFileChannel lockForReading(final Path path) throws IOException {

		return keyLockState(path, policy).acquireRead();
	}

	/**
	 * Acquires a write lock for the specified key. Only one thread can hold a
	 * write lock for a key at a time, and no readers can hold locks.
	 *
	 * @param path
	 * 		the file path to lock for writing
	 *
	 * @return a {@link LockedChannel} that must be closed when done
	 *
	 * @throws IOException
	 * 		if acquiring the file lock fails
	 */
	public LockedFileChannel lockForWriting(final Path path) throws IOException {

		return keyLockState(path, policy).acquireWrite();
	}

	/**
	 * Returns the number of keys currently being tracked.
	 *
	 * @return the number of keys with associated locks
	 */
	int size() {

		return locks.size();
	}
}
