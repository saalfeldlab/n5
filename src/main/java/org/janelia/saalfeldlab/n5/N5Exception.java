package org.janelia.saalfeldlab.n5;

import java.io.IOException;

public class N5Exception extends RuntimeException{

	public N5Exception() {

		super();
	}

	public N5Exception(String message) {

		super(message);
	}

	public N5Exception(String message, Throwable cause) {

		super(message, cause);
	}

	public N5Exception(Throwable cause) {

		super(cause);
	}

	protected N5Exception(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {

		super(message, cause, enableSuppression, writableStackTrace);
	}

	public static class N5IOException extends N5Exception {

		public N5IOException(String message) {

			super(message);
		}

		public N5IOException(String message, IOException cause) {

			super(message, cause);
		}

		public N5IOException(IOException cause) {

			super(cause);
		}

		protected N5IOException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {

			super(message, cause, enableSuppression, writableStackTrace);
		}


	}

}
