package org.janelia.saalfeldlab.n5;

public class N5Exception extends RuntimeException {

	public N5Exception() {

		super();
	}

	public N5Exception(final String message) {

		super(message);
	}

	public N5Exception(final String message, final Throwable cause) {

		super(message, cause);
	}

	public N5Exception(final Throwable cause) {

		super(cause);
	}

	protected N5Exception(
			final String message,
			final Throwable cause,
			final boolean enableSuppression,
			final boolean writableStackTrace) {

		super(message, cause, enableSuppression, writableStackTrace);
	}

	public static class N5IOException extends N5Exception {

		public N5IOException(final String message) {

			super(message);
		}

		public N5IOException(final String message, final Throwable cause) {

			super(message, cause);
		}

		public N5IOException(final Throwable cause) {

			super(cause);
		}

		protected N5IOException(
				final String message,
				final Throwable cause,
				final boolean enableSuppression,
				final boolean writableStackTrace) {

			super(message, cause, enableSuppression, writableStackTrace);
		}
	}

	/**
	 * This excpetion represents the situation when an attribute is requested by key as a specific Class,
	 * 	that attribute key <b>does</b> exist, but is not parseable as the desired Class
	 */
	public static class N5ClassCastException extends N5Exception {


		public N5ClassCastException(final Class<?> cls) {

			super("Cannot cast as class " + cls.getName());
		}

		public N5ClassCastException(final String message) {

			super(message);
		}

		public N5ClassCastException(final String message, final Throwable cause) {

			super(message, cause);
		}

		public N5ClassCastException(final Throwable cause) {

			super(cause);
		}

		protected N5ClassCastException(
				final String message,
				final Throwable cause,
				final boolean enableSuppression,
				final boolean writableStackTrace) {

			super(message, cause, enableSuppression, writableStackTrace);
		}
	}

	public static class N5NoSuchKeyException extends N5IOException {

		public N5NoSuchKeyException(final String message) {

			super(message);
		}

		public N5NoSuchKeyException(final String message, final Throwable cause) {

			super(message, cause);
		}

		public N5NoSuchKeyException(final Throwable cause) {

			super(cause);
		}

		protected N5NoSuchKeyException(
				final String message,
				final Throwable cause,
				final boolean enableSuppression,
				final boolean writableStackTrace) {

			super(message, cause, enableSuppression, writableStackTrace);
		}
	}
}
