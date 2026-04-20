package org.janelia.saalfeldlab.n5;

/**
 * File locking policy.
 * <p>
 * Usually, we want to coordinate reads and writs to a container such that
 * <ul>
 * <li>multiple readers can access a key simultaneously (blocking all writers).</li>
 * <li>A writer should have exclusive access to a key (blocking all other readers and writers).</li>
 * </ul>
 * However, this cannot always be enforced for all backends:
 * For SMB on macOS OS-level file locking is broken.
 * For AWS S3 and Google Cloud we can detect (but not prevent) concurrent modifications.
 * <p>
 * Sometimes we know that we can disregard locking, for example in read-only settings.
 * <p>
 * {@code IoPolicy} can be used to configure locking policy for backends ({@link
 * KeyValueAccess}) that support various locking strategies (potentially coming
 * with performance trade-offs).
 * <p>
 * The policy values ({@link #STRICT}, {@link #UNSAFE}, {@link #PERMISSIVE})
 * specify intent. Detailed interpretation is up to the backend implementation.
 */
public enum LockingPolicy {

	/**
	 * Protect all reads and writes by locks.
	 * Fail if locking is not possible.
	 */
	STRICT,

	/**
	 * Reads and writes are unprotected.
	 */
	UNSAFE,

	/**
	 * Try to lock for all reads and writes.
	 * Fall back to unprotected reads and writes if locking is not possible.
	 * This is the default.
	 */
	PERMISSIVE;

	static LockingPolicy fromString(final String s) {
		if ("strict".equalsIgnoreCase(s))
			return STRICT;
		else if ("unsafe".equalsIgnoreCase(s))
			return UNSAFE;
//		else if ("permissive".equalsIgnoreCase(s))
//			return PERMISSIVE;
		else
			return PERMISSIVE;
	}
}
