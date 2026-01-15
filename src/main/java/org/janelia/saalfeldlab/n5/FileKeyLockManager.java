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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A lock manager that provides both thread-safe and process-safe read/write
 * locking for filesystem keys.
 * <p>
 * This class coordinates two levels of locking:
 * <ul>
 *   <li><b>Thread-level:</b> Uses {@link ReentrantReadWriteLock} to coordinate
 *       access among threads within the same JVM.</li>
 *   <li><b>Process-level:</b> Uses {@link FileLock} to coordinate access among
 *       different processes/JVMs.</li>
 * </ul>
 * <p>
 * For reading:
 * <ul>
 *   <li>The first thread to acquire a read lock also acquires a shared file lock.</li>
 *   <li>Subsequent threads acquire only the internal read lock (no duplicate file lock).</li>
 *   <li>When the last reader releases, the file lock is also released.</li>
 * </ul>
 * <p>
 * For writing:
 * <ul>
 *   <li>The writer acquires an exclusive file lock (after all readers have released).</li>
 *   <li>When the write lock is released, the file lock is also released.</li>
 * </ul>
 */
public class FileKeyLockManager {

	private final ConcurrentHashMap<String, KeyLockState> locks = new ConcurrentHashMap<>();

	/**
	 * Creates a new FileSystemKeyLockManager.
	 */
	public FileKeyLockManager() {

	}

	/**
	 * Acquires a read lock for the specified key. Multiple threads can hold
	 * read locks for the same key simultaneously.
	 * <p>
	 * The first reader will acquire a shared file lock. Subsequent readers
	 * only acquire the thread-level lock.
	 *
	 * @param path
	 *            the key (file path) to lock for reading
	 * @return a {@link LockedChannel} that must be closed when done
	 * @throws IOException
	 *             if acquiring the file lock fails
	 */
	public LockedFileChannel lockForReading(final Path path) throws IOException {

		final String key = path.toAbsolutePath().toString();
		final KeyLockState state = locks.computeIfAbsent(key, k -> new KeyLockState(path));

		state.lockForReading();

		final FileChannel channel;
		try {
			channel = FileChannel.open(path, StandardOpenOption.READ);
		} catch (final IOException e) {
			state.releaseLock();
			throw e;
		}

		return new LockedFileChannel(state, channel);
	}

	/**
	 * Acquires a write lock for the specified key. Only one thread can hold a
	 * write lock for a key at a time, and no readers can hold locks.
	 *
	 * @param path
	 *            the file path to lock for writing
	 * @return a {@link LockedChannel} that must be closed when done
	 * @throws IOException
	 *             if acquiring the file lock fails
	 */
	public LockedFileChannel lockForWriting(final Path path) throws IOException {

		final String key = path.toAbsolutePath().toString();
		final KeyLockState state = locks.computeIfAbsent(key, k -> new KeyLockState(path));

		state.lockForWriting();

		final FileChannel channel;
		try {
			channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
		} catch (final IOException e) {
			state.releaseLock();
			throw e;
		}

		return new LockedFileChannel(state, channel);
	}

	/**
	 * Attempts to acquire a read lock for the specified key without blocking.
	 *
	 * @param path
	 *            the file path to lock for reading
	 * @return a {@link LockedChannel} if the lock was acquired, null otherwise
	 */
	public LockedFileChannel tryLockForReading(final Path path) {

		final String key = path.toAbsolutePath().toString();
		final KeyLockState state = locks.computeIfAbsent(key, k -> new KeyLockState(path));

		if (!state.tryLockForReading())
			return null;

		final FileChannel channel;
		try {
			channel = FileChannel.open(path, StandardOpenOption.READ);
		} catch (final IOException e) {
			state.releaseLock();
			return null;
		}

		return new LockedFileChannel(state, channel);
	}

	/**
	 * Attempts to acquire a write lock for the specified key without blocking.
	 *
	 * @param path
	 *            the file path to lock for writing
	 * @return a {@link LockedChannel} if the lock was acquired, null otherwise
	 */
	public LockedFileChannel tryLockForWriting(final Path path) {

		final String key = path.toAbsolutePath().toString();
		final KeyLockState state = locks.computeIfAbsent(key, k -> new KeyLockState(path));

		if (!state.tryLockForWriting())
			return null;


		final FileChannel channel;
		try {
			channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
		} catch (final IOException e) {
			state.releaseLock();
			return null;
		}

		return new LockedFileChannel(state, channel);
	}

	/**
	 * Returns the number of keys currently being tracked.
	 *
	 * @return the number of keys with associated locks
	 */
	public int size() {

		return locks.size();
	}

	/**
	 * Removes the lock for a key if it is not currently held.
	 *
	 * @param key
	 *            the key whose lock should be removed
	 * @return true if the lock was removed, false if it's currently in use
	 */
	public boolean removeLockIfUnused(final String key) {

		final KeyLockState state = locks.get(key);
		if (state == null) {
			return false;
		}

		synchronized (state) {
			if (!state.isLocked())
				return locks.remove(key, state);
		}
		return false;
	}

	/**
	 * Clears all unused locks from the lock map.
	 *
	 * @return the number of locks that were removed
	 */
	public int clearUnusedLocks() {

		int removed = 0;
		for (final String key : locks.keySet()) {
			if (removeLockIfUnused(key)) {
				removed++;
			}
		}
		return removed;
	}

}
