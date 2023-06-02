package org.janelia.saalfeldlab.n5;

import java.io.IOException;

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

		public N5IOException(final String message, final IOException cause) {

			super(message, cause);
		}

		public N5IOException(final IOException cause) {

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
}
