package org.janelia.saalfeldlab.n5.codec.dataset;

import java.util.function.DoubleUnaryOperator;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.codec.dataset.CastValueCodecInfo.Rounding;

/**
 * Resolves a {@link Rounding} mode to a {@link DoubleUnaryOperator}, once, so the
 * mode switch stays out of the element loop. Shared by {@link CastValueCodec} and
 * {@link ScaleOffsetCastCodec}: both select their rounder at construction time,
 * making the call site monomorphic for a given codec instance so it inlines.
 */
class Rounders {

	private Rounders() {}

	static DoubleUnaryOperator forRounding(final Rounding rounding) {

		switch (rounding) {
		case NEAREST_EVEN:
			return Math::rint;
		case TOWARDS_ZERO:
			return value -> value < 0 ? Math.ceil(value) : Math.floor(value);
		case TOWARDS_POSITIVE:
			return Math::ceil;
		case TOWARDS_NEGATIVE:
			return Math::floor;
		case NEAREST_AWAY:
			return Rounders::roundNearestAway;
		default:
			throw new N5IOException("Unsupported rounding mode " + rounding);
		}
	}

	private static double roundNearestAway(final double value) {

		final double magnitude = Math.abs(value);
		double result = Math.floor(magnitude + 0.5);
		// guard the case where magnitude + 0.5 rounds up on its own
		if (result - magnitude > 0.5)
			result -= 1.0;
		return Math.copySign(result, value);
	}
}
