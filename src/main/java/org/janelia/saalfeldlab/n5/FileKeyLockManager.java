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
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides thread-safe and process-safe read/write locking for filesystem paths.
 * Uses thread locks for JVM coordination and file locks for inter-process coordination.
 */
class FileKeyLockManager {

	static final FileKeyLockManager FILE_LOCK_MANAGER = new FileKeyLockManager();

	private static final ReferenceQueue<LockedFileChannel> queue = new ReferenceQueue<>();
	private static final Set<LockedChannelReference> phantomRefs = ConcurrentHashMap.newKeySet();
	private static final ConcurrentHashMap<String, KeyLockState> locks = new ConcurrentHashMap<>();

	/**
	 * PhantomReference for LockedFileChannel that releases the lock if the
	 * channel is garbage collected without being closed.
	 */
	private static class LockedChannelReference extends PhantomReference<LockedFileChannel> {

		private final String key;
		private final KeyLockState state;

		LockedChannelReference(final LockedFileChannel referent, final String key, final KeyLockState state) {
			super(referent, queue);
			this.key = key;
			this.state = state;
		}

		void cleanup() {
			state.releaseFileLockForCleanup();
			phantomRefs.remove(this);
			/* always remove from map - if the LockedFileChannel was GC'd, the entry is stale */
			locks.remove(key, state);
		}
	}

	private void clearQueue() {
		Reference<? extends LockedFileChannel> ref;
		while ((ref = queue.poll()) != null) {
			((LockedChannelReference)ref).cleanup();
		}
	}

	private class ManagedLockedFileChannel extends LockedFileChannel {

		private final String key;
		private final KeyLockState state;

		ManagedLockedFileChannel(final KeyLockState state, final FileChannel channel, final String key) {
			super(state, channel);
			this.key = key;
			this.state = state;
		}

		@Override
		public void close() throws IOException {
			super.close();
			synchronized (state) {
				if (!state.isLocked()) {
					locks.remove(key, state);
				}
			}
			clearQueue();
		}
	}

	private FileKeyLockManager() {
	}

	private LockedFileChannel registerLockedFileChannel(final FileChannel channel, final String key, final KeyLockState state) {
		final LockedFileChannel lockedChannel = new ManagedLockedFileChannel(state, channel, key);
		phantomRefs.add(new LockedChannelReference(lockedChannel, key, state));
		return lockedChannel;
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
		/* if there are stale entries in the queue, clean them before we try to lock */
		clearQueue();

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
		return registerLockedFileChannel(channel, key, state);
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
		/* if there are stale entries in the queue, clean them before we try to lock */
		clearQueue();

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

		return registerLockedFileChannel(channel, key, state);
	}

	/**
	 * Attempts to acquire a read lock for the specified key without blocking.
	 *
	 * @param path
	 *            the file path to lock for reading
	 * @return a {@link LockedChannel} if the lock was acquired, null otherwise
	 */
	public LockedFileChannel tryLockForReading(final Path path) {
		/* if there are stale entries in the queue, clean them before we try to lock */
		clearQueue();

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

		return registerLockedFileChannel(channel, key, state);
	}

	/**
	 * Attempts to acquire a write lock for the specified key without blocking.
	 *
	 * @param path
	 *            the file path to lock for writing
	 * @return a {@link LockedChannel} if the lock was acquired, null otherwise
	 */
	public LockedFileChannel tryLockForWriting(final Path path) {
		/* if there are stale entries in the queue, clean them before we try to lock */
		clearQueue();

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

		return registerLockedFileChannel(channel, key, state);
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
	 * Removes the lock state for a key if no locks are held.
	 *
	 * @param key the key whose lock state should be removed
	 * @return true if removed, false if currently in use or not found
	 */
	public boolean removeLockIfUnused(final String key) {

		final KeyLockState state = locks.get(key);
		if (state == null)
			return false;

		synchronized (state) {
			if (!state.isLocked())
				return locks.remove(key, state);
		}
		return false;
	}
}
