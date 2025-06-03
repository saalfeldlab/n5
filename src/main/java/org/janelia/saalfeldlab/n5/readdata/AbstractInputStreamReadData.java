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
package org.janelia.saalfeldlab.n5.readdata;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

// not thread-safe
abstract class AbstractInputStreamReadData implements ReadData {

	private ByteArraySplittableReadData bytes;

	@Override
	public ReadData materialize() throws N5IOException {
		if (bytes == null) {
			final byte[] data;
			final int length = (int) length();
			if (length >= 0) {
				data = new byte[length];
				try( InputStream is = inputStream())  {
					new DataInputStream(is).readFully(data);
				} catch (IOException e) {
					throw new N5IOException(e);
				}
			} else {
				try( InputStream is = inputStream())  {
					data = IOUtils.toByteArray(is);
				} catch (IOException e) {
					throw new N5IOException(e);
				}
			}
			bytes = new ByteArraySplittableReadData(data);
		}
		return bytes;
	}

	@Override
	public byte[] allBytes() throws N5IOException, IllegalStateException {
		return materialize().allBytes();
	}
}
