package org.janelia.saalfeldlab.n5.util;

/*
 * An immutable {@Position}.
 */
public class FinalPosition implements Position {

	public final long[] position;

	public FinalPosition(long[] position) {
		this.position = position;
	}

	public FinalPosition(Position p) {
		this.position = p.get().clone();
	}

	@Override
	public long[] get() {
		return position;
	}

	@Override
	public long get(int i) {
		return position[i];
	}

	@Override
	public String toString() {
		return Position.toString(this);
	}

	@Override
	public boolean equals(Object obj) {
		return Position.equals(this, obj);
	}

}
