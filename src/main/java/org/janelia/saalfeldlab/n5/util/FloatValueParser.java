package org.janelia.saalfeldlab.n5.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.janelia.saalfeldlab.n5.N5Exception;

/**
 * Parses {@link Float} and {@link Double} values from JSON hex strings.
 * <p>
 * This does not directly cover the Strings "NaN", "Infinity", and "-Infinity"
 * but they are parsable by the parseDouble and parseFloat methods. Rather, this
 * class handles converting to and from the hex representations of NaN,
 * -Infinity, Infinity, and all other allowable values.
 */
public class FloatValueParser {

	/**
	 * Parses a hex string to a float value.
	 * 
	 * @param hexString
	 *            hex string in format "0x" followed by 8 hex digits
	 * @return the float value
	 * @throws N5Exception
	 *             if the string format is invalid
	 */
	public static float parseFloat(String hexString) throws N5Exception {

		validateFloat(hexString);
		final int intValue = Integer.parseUnsignedInt(hexString.substring(2), 16);
		return Float.intBitsToFloat(intValue);
	}

	/**
	 * Encodes a float value to a hex string.
	 * 
	 * @param value
	 *            the float to encode
	 * @return hex string in format "0x" followed by 8 hex digits
	 */
	public static String encodeFloat(float value) {

		return String.format("0x%08x", Float.floatToIntBits(value));
	}

	private static void validateFloat(String hexString) {

		if (!hexString.startsWith("0x") || hexString.length() != 10)
			throw new N5Exception("Could not parse string " + hexString + " as float.");
	}

	/**
	 * Parses a hex string to a double value.
	 * 
	 * @param hexString
	 *            hex string in format "0x" followed by 16 hex digits
	 * @return the double value
	 * @throws N5Exception
	 *             if the string format is invalid
	 */
	public static double parseDouble(String hexString) throws N5Exception {

		validateDouble(hexString);
		final long longValue = Long.parseUnsignedLong(hexString.substring(2), 16);
		return Double.longBitsToDouble(longValue);
	}

	/**
	 * Encodes a double value to a hex string.
	 * 
	 * @param value
	 *            the double to encode
	 * @return hex string in format "0x" followed by 16 hex digits
	 */
	public static String encodeDouble(double value) {

		return String.format("0x%016x", Double.doubleToLongBits(value));
	}

	private static void validateDouble(String hexString) {

		if (!hexString.startsWith("0x") || hexString.length() != 18)
			throw new N5Exception("Could not parse string " + hexString + " as double.");
	}

	/**
	 * Parses a hex string to a byte array.
	 * 
	 * @param hexString
	 *            hex string in format "0x" followed by hex digits
	 * @return the decoded byte array
	 * @throws N5Exception
	 *             if the string format is invalid or decoding fails
	 */
	public static byte[] parseBytes(String hexString) throws N5Exception {

		validateBytes(hexString);
		try {
			return Hex.decodeHex(hexString.substring(2));
		} catch (DecoderException e) {
			throw new N5Exception(e);
		}
	}

	private static void validateBytes(String hexString) {

		if (!hexString.startsWith("0x"))
			throw new N5Exception("Could not parse string " + hexString + " to bytes.");
	}

}
