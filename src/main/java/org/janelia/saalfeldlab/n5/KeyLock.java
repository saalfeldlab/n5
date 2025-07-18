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

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;

/**
 * A lock manager that provides thread-safe read/write locking for keys.
 * 
 * This class manages a set of {@link ReentrantReadWriteLock}s, one per key,
 * allowing multiple threads to read the same key simultaneously while ensuring
 * exclusive access for writes.
 *
 * Unlike file locks which operate at the process level, this provides
 * thread-level locking within a single JVM.
 */
public class KeyLock {

	private final ConcurrentHashMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();

	/**
	 * Acquires a read lock for the specified key. Multiple threads can hold
	 * read locks for the same key simultaneously.
	 * 
	 * @param key
	 *            the key to lock for reading
	 * @return a {@link Lock} that must be unlocked when done
	 */
	public Lock lockForReading(String key) {

		ReentrantReadWriteLock rwLock = locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
		Lock readLock = rwLock.readLock();
		readLock.lock();
		return readLock;
	}

	/**
	 * Acquires a write lock for the specified key. Only one thread can hold a
	 * write lock for a key at a time.
	 * 
	 * @param key
	 *            the key to lock for writing
	 * @return a {@link Lock} that must be unlocked when done
	 */
	public Lock lockForWriting(String key) {

		ReentrantReadWriteLock rwLock = locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
		Lock writeLock = rwLock.writeLock();
		writeLock.lock();
		return writeLock;
	}

	/**
	 * Attempts to acquire a read lock for the specified key without blocking.
	 * 
	 * @param key
	 *            the key to lock for reading
	 * @return a {@link Lock} if the lock was acquired, null otherwise
	 */
	public Lock tryLockForReading(String key) {

		ReentrantReadWriteLock rwLock = locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
		Lock readLock = rwLock.readLock();
		if (readLock.tryLock()) {
			return readLock;
		}
		return null;
	}

	/**
	 * Attempts to acquire a write lock for the specified key without blocking.
	 *
	 * @param key
	 *            the key to lock for writing
	 * @return a {@link Lock} if the lock was acquired, null otherwise
	 */
	public Lock tryLockForWriting(String key) {

		ReentrantReadWriteLock rwLock = locks.computeIfAbsent(key, k -> new ReentrantReadWriteLock());
		Lock writeLock = rwLock.writeLock();
		if (writeLock.tryLock()) {
			return writeLock;
		}
		return null;
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
	 * Removes the lock for a key if it is not currently held. This can be used
	 * to clean up unused locks to prevent memory leaks.
	 * 
	 * @param key
	 *            the key whose lock should be removed
	 * @return true if the lock was removed, false if it's currently in use
	 */
	public boolean removeLockIfUnused(String key) {

		ReentrantReadWriteLock rwLock = locks.get(key);
		if (rwLock != null && !rwLock.isWriteLocked() && rwLock.getReadLockCount() == 0) {
			return locks.remove(key, rwLock);
		}
		return false;
	}

	/**
	 * Returns an {@link Optional} containing the lock for the given
	 * key, if it exists.
	 * <p>
	 * This method can be useful for monitoring and debugging. For example,
	 * to check how many threads are currently reading a key:
	 * <pre>{@code
	 * keyLock.getKeyLock("myKey").ifPresent(lock -> {
	 *     System.out.println("Key 'myKey' has " + lock.getReadLockCount() + " readers");
	 * });
	 * }</pre>
	 *
	 * @param key
	 * 	the key whose lock will be returned
	 * @return an {@link Optional} containing the {@link ReentrantReadWriteLock}
	 *         for the key, or empty if no lock exists for the key
	 */
	public Optional<ReentrantReadWriteLock> getKeyLock(String key) {
		return Optional.ofNullable(locks.get(key));
	}

	/**
	 * Clears all unused locks from the lock map. Locks that are currently held
	 * will not be removed.
	 * 
	 * @return the number of locks that were removed
	 */
	public int clearUnusedLocks() {

		int removed = 0;
		for (String key : locks.keySet()) {
			if (removeLockIfUnused(key)) {
				removed++;
			}
		}
		return removed;
	}
}