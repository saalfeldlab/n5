package org.janelia.saalfeldlab.n5.demo;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class AttributePathDemo {

	final Gson gson;

	public AttributePathDemo() {
		gson = new Gson();
	}

	public static void main(String[] args) throws IOException {

		new AttributePathDemo().demo();
	}

	public void demo() throws IOException {

		final String rootPath = "/home/john/projects/n5/demo.n5";
		final N5FSWriter n5 = new N5FSWriter( rootPath );
		final N5FSWriter n5WithNulls = new N5FSWriter( rootPath, new GsonBuilder().serializeNulls() );

		final String group = "";
		final String specialGroup = "specialCharsGroup";
		final String rmAndNulls = "rmAndNulls";
		n5.createGroup(group);
		n5.createGroup(specialGroup);
		n5.createGroup(rmAndNulls);

		// clear all attributes
		n5.setAttribute(group, "/", new JsonObject());
		n5.setAttribute(specialGroup, "/", new JsonObject());
		n5.setAttribute(rmAndNulls, "/", new JsonObject());

		simple(n5, group);
		arrays(n5, group);
		objects(n5, group);
		specialChars(n5, specialGroup);
		removingAttributesAndNulls(n5, rmAndNulls);
		removingAttributesAndNulls(n5WithNulls, rmAndNulls);

		n5.close();
	}

	public void simple(final N5FSWriter n5, final String group) throws IOException {
		n5.setAttribute(group, "six", 6);
		System.out.println(n5.getAttribute("/", "six", Integer.class)); // 6
		System.out.println(n5.getAttribute("/", "twelve", Integer.class)); // null

		final String longKey = "The Answer to the Ultimate Question";
		n5.setAttribute(group, longKey, 42);
		System.out.println(n5.getAttribute(group, longKey, Integer.class)); // 42

		n5.setAttribute(group, "name", "Marie Daly");
		System.out.println(n5.getAttribute(group, "name", String.class)); // returns "Marie Daly"
		System.out.println(n5.getAttribute(group, "name", int.class)); // returns null

		n5.setAttribute(group, "year", "1921");
		System.out.println( "(String):" + n5.getAttribute(group, "year", String.class)); 
		System.out.println( "(int)   :" + n5.getAttribute(group, "year", int.class)); 

		n5.setAttribute(group, "animal", "aardvark");
		System.out.println( n5.getAttribute(group, "animal", String.class)); // "aardvark"
		n5.setAttribute(group, "animal", new String[]{"bat", "cat", "dog"}); // overwrites "animal"
		printAsJson( n5.getAttribute(group, "animal", String[].class)); // ["bat", "cat", "dog"]

		System.out.println(getRawJson(n5, group));
		// {"six":6,"The Answer to the Ultimate Question":42,"name":"Marie Daly","year":"1921","animal":["bat","cat","dog"]}

		n5.setAttribute(group, "/", new JsonObject()); // overwrites "animal"
		System.out.println(getRawJson(n5, group));
		// {}
	}

	public void arrays(final N5FSWriter n5, final String group) throws IOException {

		n5.setAttribute(group, "array", new double[] { 5, 6, 7, 8 });
		System.out.println( Arrays.toString(n5.getAttribute(group, "array", double[].class))); // [5.0, 6.0, 7.0, 8.0]
		System.out.println( n5.getAttribute(group, "array[0]", double.class)); // 7.0
		System.out.println( n5.getAttribute(group, "array[2]", double.class)); // 7.0
		System.out.println( n5.getAttribute(group, "array[999]", double.class)); // null
		System.out.println( n5.getAttribute(group, "array[-1]", double.class)); // null 


		n5.setAttribute(group, "array[1]", 0.6);
		System.out.println( Arrays.toString(n5.getAttribute(group, "array", double[].class))); // [5.0, 0.6, 7.0, 8.0]
		n5.setAttribute(group, "array[6]", 99.99 );
		System.out.println( Arrays.toString(n5.getAttribute(group, "array", double[].class))); // [5.0, 0.6, 7.0, 8.0, 0.0, 0.0, 99.99]
		n5.setAttribute(group, "array[-5]", -5 );
		System.out.println( Arrays.toString(n5.getAttribute(group, "array", double[].class))); // [5.0, 0.6, 7.0, 8.0, 0.0, 0.0, 99.99]

		System.out.println( n5.getAttribute(group, "array", int.class)); // [5.0, 0.6, 7.0, 8.0, 0.0, 0.0, 99.99]
	}

	@SuppressWarnings("rawtypes")
	public void objects(final N5FSWriter n5, final String group) throws IOException {
		Map a = Collections.singletonMap("a", "A");
		Map b = Collections.singletonMap("b", "B");
		Map c = Collections.singletonMap("c", "C");

		n5.setAttribute(group, "obj", a ); 
		printAsJson(n5.getAttribute(group, "obj", Map.class)); // {"a":"A"}
		System.out.println("");

		n5.setAttribute(group, "obj/a", b);
		printAsJson(n5.getAttribute(group, "obj", Map.class)); 	 // {"a": {"b": "B"}}
		printAsJson(n5.getAttribute(group, "obj/a", Map.class)); // {"b": "B"}
		System.out.println("");

		n5.setAttribute(group, "obj/a/b", c);
		printAsJson(n5.getAttribute(group, "obj", Map.class));     // {"a": {"b": {"c": "C"}}}
		printAsJson(n5.getAttribute(group, "obj/a", Map.class));   // {"b": {"c": "C"}}
		printAsJson(n5.getAttribute(group, "obj/a/b", Map.class)); // {"c": "C"}
		printAsJson(n5.getAttribute(group, "/", Map.class)); // returns {"obj": {"a": {"b": {"c": "C"}}}}
		System.out.println("");

		n5.setAttribute(group, "pet", new Pet("Pluto", 93));
		System.out.println(n5.getAttribute(group, "pet", Pet.class)); 	// Pet("Pluto", 93)
		printAsJson(n5.getAttribute(group, "pet", Map.class)); 			// {"name": "Pluto", "age": 93}

		n5.setAttribute(group, "pet/likes", new String[]{"Micky"});
		printAsJson(n5.getAttribute(group, "pet", Map.class)); // {"name": "Pluto", "age": 93, "likes": ["Micky"]}
		System.out.println("");

		n5.removeAttribute(group, "/");
		System.out.println(getRawJson(n5, group)); // null

		n5.setAttribute(group, "one/[2]/three/[4]", 5);
		System.out.println(getRawJson(n5, group)); // {"one":[null,null,{"three":[0,0,0,0,5]}]}
	}

	public void specialChars(final N5FSWriter n5, final String group) throws IOException {
		n5.setAttribute(group, "\\/", "fwdSlash");
		printAsJson(n5.getAttribute(group, "\\/", String.class ));   // "fwdSlash"

		n5.setAttribute(group, "\\\\", "bckSlash");
		printAsJson(n5.getAttribute(group, "\\\\", String.class ));   // "bckSlash"

		// print out the contents of attributes.json
		System.out.println("\n" + getRawJson(n5, group)); // {"/":"fwdSlash","\\\\":"bckSlash"}
	}

	public void removingAttributesAndNulls(final N5FSWriter n5, final String group) throws IOException {

		n5.setAttribute(group, "cow", "moo");
		n5.setAttribute(group, "dog", "woof");
		n5.setAttribute(group, "sheep", "baa");
		System.out.println(getRawJson(n5, group)); // {"sheep":"baa","cow":"moo","dog":"woof"}

		n5.removeAttribute(group, "cow"); // void method
		System.out.println(getRawJson(n5, group)); // {"sheep":"baa","dog":"woof"}

		String theDogSays = n5.removeAttribute(group, "dog", String.class); // returns type
		System.out.println(theDogSays);				// woof
		System.out.println(getRawJson(n5, group));  // {"sheep":"baa"}

		n5.removeAttribute(group, "sheep", int.class); // returns type
		System.out.println(getRawJson(n5, group)); // {"sheep":"baa"}

		System.out.println( n5.removeAttribute(group, "sheep", String.class)); // "baa" 
		System.out.println(getRawJson(n5, group)); // {}

		n5.setAttribute(group, "attr", "value");
		System.out.println(getRawJson(n5, group)); // {"attr":"value"}
		n5.setAttribute(group, "attr", null);
		System.out.println(getRawJson(n5, group)); // if serializeNulls {"attr":null}

		n5.setAttribute(group, "foo", 12);
		System.out.println(getRawJson(n5, group)); // {"foo":12}
		n5.removeAttribute(group, "foo");
		System.out.println(getRawJson(n5, group)); // {}
	}

	public String getRawJson(final N5Reader n5, final String group) throws IOException {
		return new String(
				Files.readAllBytes(
						Paths.get(Paths.get(n5.getURI()).toAbsolutePath().toString(), group, "attributes.json")),
				Charset.defaultCharset());
	}

	public void printAsJson(final Object obj) {
		System.out.println(gson.toJson(obj));
	}

	class Pet {
		String name;
		int age;

		public Pet(String name, int age) {
			this.name = name;
			this.age = age;
		}

		public String toString() {
			return String.format("pet %s is %d", name, age);
		}
	}

}
