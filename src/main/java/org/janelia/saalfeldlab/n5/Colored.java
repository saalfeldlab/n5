package org.janelia.saalfeldlab.n5;

public class Colored {

	public static final Colored green = new Colored("\033[0;92m");
	public static final Colored blue = new Colored("\033[0;94m");
	public static final Colored cyan = new Colored("\033[0;96m");
	public static final Colored yellow = new Colored("\033[0;93m");
	public static final Colored red = new Colored("\033[0;91m");
	public static final Colored magenta = new Colored("\033[0;95m");

	private final String setColor;
	private final String resetColor = "\033[39;49m";

	private Colored(final String setColorSequence) {
		this.setColor = setColorSequence;
	}

	public void println(String s) {
		System.out.println(setColor + s + resetColor);
	}
}

