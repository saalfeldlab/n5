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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

/**
 * A ReadData implementation that concatenates multiple ReadData instances.
 */
public class ConcatenatedReadData implements ReadData {

	private final List<ReadData> parts;
	private Long cachedLength = null;

	/**
	 * Create a new ConcatenatedReadData from multiple ReadData instances.
	 *
	 * @param parts the ReadData instances to concatenate
	 */
	public ConcatenatedReadData(final ReadData... parts) {
		this(Arrays.asList(parts));
	}

	/**
	 * Create a new ConcatenatedReadData from a list of ReadData instances.
	 *
	 * @param parts the ReadData instances to concatenate
	 */
	public ConcatenatedReadData(final List<ReadData> parts) {
		if (parts == null) {
			throw new IllegalArgumentException("Parts cannot be null");
		}
		this.parts = new ArrayList<>(parts);
	}

	@Override
	public long length() throws N5IOException {
		if (cachedLength != null) {
			return cachedLength;
		}

		long totalLength = 0;
		for (final ReadData part : parts) {
			final long partLength = part.length();
			if (partLength < 0) {
				return -1;
			}
			totalLength += partLength;
		}
		cachedLength = totalLength;
		return totalLength;
	}

	@Override
	public InputStream inputStream() throws N5IOException, IllegalStateException {
		final List<InputStream> streams = new ArrayList<>();
		for (final ReadData part : parts) {
			streams.add(part.inputStream());
		}
		return new SequenceInputStream(Collections.enumeration(streams));
	}

	@Override
	public byte[] allBytes() throws N5IOException, IllegalStateException {

		if (parts.size() == 1)
			return parts.get(0).allBytes();

		final long totalLength = length();
		if (totalLength < 0) {
			final List<byte[]> allParts = new ArrayList<>();
			long actualLength = 0;
			for (final ReadData part : parts) {
				final byte[] partBytes = part.allBytes();
				allParts.add(partBytes);
				actualLength += partBytes.length;
			}
			return concatenateBytes(allParts, actualLength);
		} else if (totalLength > Integer.MAX_VALUE) {
			throw new N5IOException("ConcatenatedReadData too large to fit in byte array: " + totalLength);
		} else {
			final byte[] result = new byte[(int)totalLength];
			int offset = 0;
			for (final ReadData part : parts) {
				final byte[] partBytes = part.allBytes();
				System.arraycopy(partBytes, 0, result, offset, partBytes.length);
				offset += partBytes.length;
			}
			return result;
		}
	}

	private byte[] concatenateBytes(final List<byte[]> arrays, final long totalLength) {
		if (totalLength > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Total length exceeds maximum array size");
		}
		final byte[] result = new byte[(int)totalLength];
		int offset = 0;
		for (final byte[] array : arrays) {
			System.arraycopy(array, 0, result, offset, array.length);
			offset += array.length;
		}
		return result;
	}

	@Override
	public ReadData materialize() throws N5IOException {
		return new ByteArrayReadData(allBytes());
	}

	@Override
	public void writeTo(final OutputStream outputStream) throws N5IOException, IllegalStateException {
		for (final ReadData part : parts) {
			part.writeTo(outputStream);
		}
	}

	@Override
	public ReadData slice(final long offset, final long length) throws N5IOException {
		if (offset < 0) {
			throw new IndexOutOfBoundsException("Offset must be non-negative");
		}

		final long totalLength = length();
		
		if (totalLength >= 0 && offset > totalLength) {
			throw new IndexOutOfBoundsException("Offset exceeds data bounds");
		}
		
		final long actualLength;
		if (length < 0) {
			if (totalLength >= 0) {
				actualLength = totalLength - offset;
			} else {
				actualLength = -1;
			}
		} else {
			actualLength = length;
			if (totalLength >= 0 && offset + actualLength > totalLength) {
				throw new IndexOutOfBoundsException("Slice extends beyond data bounds");
			}
		}

		final List<ReadData> slicedParts = new ArrayList<>();
		long currentOffset = 0;
		long remainingOffset = offset;
		long remainingLength = actualLength;

		boolean foundValidPart = false;
		for (final ReadData part : parts) {
			if (remainingLength == 0) {
				break;
			}

			final long partLength = part.length();
			if (partLength < 0) {
				return materialize().slice(offset, length);
			}

			if (remainingOffset >= partLength) {
				remainingOffset -= partLength;
				currentOffset += partLength;
				continue;
			}

			foundValidPart = true;
			final long partOffset = remainingOffset;
			final long partSliceLength;
			if (remainingLength < 0) {
				partSliceLength = -1;
			} else {
				partSliceLength = Math.min(remainingLength, partLength - partOffset);
			}
			slicedParts.add(part.slice(partOffset, partSliceLength));

			remainingOffset = 0;
			if (remainingLength > 0) {
				remainingLength -= partSliceLength;
			}
			currentOffset += partLength;
		}
		
		if (remainingOffset > 0 && !foundValidPart && actualLength != 0) {
			throw new IndexOutOfBoundsException("Offset exceeds data bounds");
		}

		if (slicedParts.isEmpty()) {
			if (actualLength == 0 && offset == totalLength) {
				// Special case: empty slice at the end should delegate to create proper bounds checking
				if (!parts.isEmpty()) {
					final ReadData lastPart = parts.get(parts.size() - 1);
					final long lastPartLength = lastPart.length();
					if (lastPartLength >= 0) {
						return lastPart.slice(lastPartLength, 0);
					}
				}
			}
			return ReadData.empty();
		} else if (slicedParts.size() == 1) {
			return slicedParts.get(0);
		} else {
			return new ConcatenatedReadData(slicedParts);
		}
	}
}