package org.janelia.saalfeldlab.n5.readdata;

import java.io.IOException;
import java.io.OutputStream;

public interface OutputStreamEncoder {

	EncodedOutputStream encode(OutputStream out) throws IOException;

	interface Finisher {

		void finish() throws IOException;
	}

	final class EncodedOutputStream {

		private final OutputStream outputStream;
		private final Finisher finisher;

		public EncodedOutputStream(final OutputStream outputStream, final Finisher finisher) {
			this.outputStream = outputStream;
			this.finisher = finisher;
		}

		OutputStream outputStream() {
			return outputStream;
		}

		void finish() throws IOException {
			finisher.finish();
		}
	}
}
