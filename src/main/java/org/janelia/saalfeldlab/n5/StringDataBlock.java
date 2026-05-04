package org.janelia.saalfeldlab.n5;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class StringDataBlock extends AbstractDataBlock<String[]> {

    protected static final Charset ENCODING = StandardCharsets.UTF_8;
    protected static final String NULLCHAR = "\0";
    protected byte[] serializedData = null;
    protected String[] actualData = null;

    public StringDataBlock(final int[] size, final long[] gridPosition, final String[] data) {
        super(size, gridPosition, new String[0], a -> a.length);
        actualData = data;
    }

    public StringDataBlock(final int[] size, final long[] gridPosition, final byte[] data) {
        super(size, gridPosition, new String[0], a -> a.length);
        serializedData = data;
    }

    public void readData(final ByteBuffer buffer) {

		if (buffer.hasArray()) {
			if (buffer.array() != serializedData)
				buffer.get(serializedData);
			actualData = deserialize(buffer.array());
		} else
			actualData = ENCODING.decode(buffer).toString().split(NULLCHAR);
    }

    protected byte[] serialize(String[] strings) {
        final String flattenedArray = String.join(NULLCHAR, strings) + NULLCHAR;
        return flattenedArray.getBytes(ENCODING);
    }

    protected String[] deserialize(byte[] rawBytes) {
        final String rawChars = new String(rawBytes, ENCODING);
        return rawChars.split(NULLCHAR);
    }

    @Override
    public int getNumElements() {
        if (serializedData == null)
            serializedData = serialize(actualData);
        return serializedData.length;
    }

    @Override
    public String[] getData() {
        if (actualData == null)
            actualData = deserialize(serializedData);
        return actualData;
    }
}
