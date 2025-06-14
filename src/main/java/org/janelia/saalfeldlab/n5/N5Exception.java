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

	/**
	 * Exception to represent an error when attempting to parse json attributes
	 */
	public static class N5JsonParseException extends N5Exception {
		public N5JsonParseException(final String message) {

			super(message);
		}

		public N5JsonParseException(final String message, final Throwable cause) {

			super(message, cause);
		}

		public N5JsonParseException(final Throwable cause) {

			super(cause);
		}

		protected N5JsonParseException(
				final String message,
				final Throwable cause,
				final boolean enableSuppression,
				final boolean writableStackTrace) {

			super(message, cause, enableSuppression, writableStackTrace);
		}
	}
}
