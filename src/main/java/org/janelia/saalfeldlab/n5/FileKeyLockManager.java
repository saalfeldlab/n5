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
package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import static org.janelia.saalfeldlab.n5.LockingPolicy.STRICT;

/**
 * Provides thread-safe and process-safe read/write locking for filesystem paths.
 * Uses thread locks for JVM coordination and file locks for inter-process coordination.
 */
class FileKeyLockManager {

	/**
	 * @deprecated use {@link FileKeyLockManager} instance per KVA instead of global one.
	 */
	@Deprecated
	static final FileKeyLockManager FILE_LOCK_MANAGER = new FileKeyLockManager(STRICT);

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
	FileKeyLockManager(final LockingPolicy policy) {
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
		if (state == null) {
			final KeyLockState newState = new KeyLockState(path, policy);
			while (state == null) {
				final WeakValue ref = locks.compute(key,
						(k, v) -> (v != null && v.get() != null)
								? v
								: new WeakValue(k, newState, refQueue));
				state = ref.get();
			}
		}
		if (state.policy() != policy) {
			throw new IOException("Trying to lock \"" + path + "\" with policy " + policy + ", but it is already used with " + state.policy());
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
