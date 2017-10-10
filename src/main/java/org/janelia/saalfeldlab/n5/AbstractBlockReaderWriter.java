/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 *
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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.commons.io.IOUtils;

public abstract class AbstractBlockReaderWriter implements BlockReader, BlockWriter {

	protected abstract InputStream getInputStream(final InputStream in) throws IOException;
	protected abstract OutputStream getOutputStream(final OutputStream out) throws IOException;

	@Override
	public <T, B extends DataBlock<T>> void read(
			final B dataBlock,
			final ReadableByteChannel channel) throws IOException {

		final ByteBuffer buffer;
		try (final InputStream in = getInputStream(Channels.newInputStream(channel))) {
			final byte[] bytes = IOUtils.toByteArray(in);
			buffer = ByteBuffer.wrap(bytes);
		}
		dataBlock.readData(buffer);
	}

	@Override
	public <T> void write(
			final DataBlock<T> dataBlock,
			final WritableByteChannel channel) throws IOException {

		final ByteBuffer buffer = dataBlock.toByteBuffer();
		try (final OutputStream out = getOutputStream(Channels.newOutputStream(channel))) {
			out.write(buffer.array());
			out.flush();

			// closing the stream will in turn cause the channel to be closed, ensure that it is properly truncated
			if (channel instanceof FileChannel)
			{
				final FileChannel fileChannel = (FileChannel)channel;
				fileChannel.truncate(fileChannel.position());
			}
		}
	}
}