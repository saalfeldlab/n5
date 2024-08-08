package org.janelia.saalfeldlab.n5;

import java.io.Serializable;

import org.janelia.saalfeldlab.n5.codec.ByteStreamCodec;
import org.janelia.saalfeldlab.n5.codec.ComposedCodec;
import org.janelia.saalfeldlab.n5.dataset.DatasetToByteStream;
import org.janelia.saalfeldlab.n5.serialization.N5NameConfig;

public interface Codec extends Serializable, N5NameConfig {

	public static boolean validCodecs(final Codec[] codecs) {

		boolean byteStreamSeen = false;
		boolean valid = true;

		int numArrayToDataset = 0;
		for (final Codec c : codecs) {

			if (c instanceof DatasetToByteStream)
				numArrayToDataset++;

			if (c instanceof ByteStreamCodec)
				byteStreamSeen = true;

			if (byteStreamSeen && !(c instanceof ByteStreamCodec))
				valid = false;

		}

		valid = valid && (numArrayToDataset == 1);
		return valid;
	}

	public static ByteStreamCodec[] extractByteCodecs(Codec[] codecs) throws IllegalArgumentException {

		int j = 0;
		ByteStreamCodec[] byteCodecs = null;
		for (int i = 0; i < codecs.length; i++) {
			final Codec c = codecs[i];
			if (c instanceof ByteStreamCodec) {
				byteCodecs = new ByteStreamCodec[codecs.length - 1 - i];
				byteCodecs[j++] = (ByteStreamCodec)c;
			} else if (byteCodecs != null)
				throw new IllegalArgumentException(
						"An array->array or array->byte codec appeared after a byte->byte codec");

			i++;
		}

		return byteCodecs;
	}

	public static ByteStreamCodec collectByteCodecs(Codec[] codecs) throws IllegalArgumentException {

		final ByteStreamCodec[] byteCodecs = extractByteCodecs(codecs);
		if (byteCodecs.length == 1)
			return (ByteStreamCodec)byteCodecs[0];
		else
			return new ComposedCodec(byteCodecs);

	}
}
