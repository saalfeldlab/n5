package org.janelia.saalfeldlab.n5.codec;

import org.apache.commons.io.input.ProxyInputStream;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.serialization.NameConfig;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Interface representing a filter can encode a {@link OutputStream}s when writing data, and decode
 * the {@link InputStream}s when reading data.
 *
 * Modeled after <a href="https://zarr.readthedocs.io/en/v2.0.1/api/codecs.html">Filters</a> in
 * Zarr.
 */
@NameConfig.Prefix("codec")
public interface Codec extends Serializable {

	public interface BytesToBytes extends Codec {

		/**
		 * Decode an {@link InputStream}.
		 *
		 * @param in
		 *            input stream
		 * @return the decoded input stream
		 */
		public InputStream decode(final InputStream in) throws IOException;

		/**
		 * Encode an {@link OutputStream}.
		 *
		 * @param out
		 *            the output stream
		 * @return the encoded output stream
		 */
		public OutputStream encode(final OutputStream out) throws IOException;
	}

	interface ArrayToBytes extends Codec {

		/**
		 * Decode an {@link InputStream}.
		 *
		 * @param in
		 *            input stream
		 * @return the DataBlock corresponding to the input stream
		 */
		public DataBlockInputStream decode(final DatasetAttributes attributes, final long[] gridPosition, final InputStream in) throws IOException;

		/**
		 * Encode a {@link DataBlock}.
		 *
		 * @param datablock the datablock to encode
		 */
		public OutputStream encode(final DatasetAttributes attributes, final DataBlock<?> datablock, final OutputStream out) throws IOException;

	}

	public abstract class DataBlockInputStream extends ProxyInputStream {


		protected DataBlockInputStream(InputStream in) {

			super(in);
		}

		public abstract DataBlock<?> allocateDataBlock() throws IOException;
	}

	public String getType();
}

