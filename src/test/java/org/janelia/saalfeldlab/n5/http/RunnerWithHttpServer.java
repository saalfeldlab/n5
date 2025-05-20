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
package org.janelia.saalfeldlab.n5.http;

import org.junit.After;
import org.junit.Before;
import org.junit.internal.runners.statements.RunAfters;
import org.junit.internal.runners.statements.RunBefores;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.model.FrameworkField;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RunnerWithHttpServer extends BlockJUnit4ClassRunner {

	private Process process;

	private final StringBuilder perTestHttpOut = new StringBuilder();

	private Path httpServerDirectory;

	private final URI httpUri = URI.create("http://localhost:8000/");

	public RunnerWithHttpServer(Class<?> klass) throws Exception {

		super(klass);
	}

	private static Path createTmpServerDirectory() throws IOException {

		/* deleteOnExit doesn't work on temporary files, so delete it manually and recreate explicitly...*/
		final Path tempDirectory = Files.createTempDirectory("n5-http-test-server-");
		tempDirectory.toFile().delete();
		tempDirectory.toFile().mkdirs();
		tempDirectory.toFile().deleteOnExit();
		return tempDirectory;
	}

	@Override protected Object createTest() throws Exception {

		final Object test = super.createTest();
		for (FrameworkField field : getTestClass().getAnnotatedFields(Parameterized.Parameter.class)) {
			if (field.getType().isAssignableFrom(Path.class)) {
				field.getField().set(test, httpServerDirectory);
			} else if (field.getType().isAssignableFrom(URI.class)) {
				field.getField().set(test, httpUri);
			}
		}
		return test;
	}

	@Override protected void runChild(FrameworkMethod method, RunNotifier notifier) {

		if (!process.isAlive()) {
			logHttpOutput();
			return;
		}

		Description description = describeChild(method);
		if (isIgnored(method)) {
			notifier.fireTestIgnored(description);
		} else {
			Statement statement = new Statement() {

				@Override
				public void evaluate() throws Throwable {

					try {
						methodBlock(method).evaluate();
					} catch (Exception e) {
						if (!process.isAlive())
							logHttpOutput();
						throw e;
					} finally {
						perTestHttpOut.setLength(0);
					}
				}
			};
			runLeaf(statement, description, notifier);
		}

	}

	private void logHttpOutput() {

		if (perTestHttpOut.length() > 0) {
			perTestHttpOut.insert(0, "Last HTTP Server Output.\n");
			perTestHttpOut.insert(0, "Http Server is not alive.\n");
			System.err.println(perTestHttpOut);
		}
	}

	@Before
	public void startHttpServer() throws Exception {

		httpServerDirectory = createTmpServerDirectory();
		ProcessBuilder processBuilder = new ProcessBuilder("python", "-m", "http.server");
		processBuilder.directory(httpServerDirectory.toFile());
		processBuilder.redirectErrorStream(true);
		process = processBuilder.start();
		waitForHttpReady();
		/* give the server some time to finish startup */
		final Thread clearStdout = new Thread(() -> {
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
				String line;
				while ((line = reader.readLine()) != null) {
					perTestHttpOut.append(line).append("\n");
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		});
		clearStdout.setDaemon(true);
		clearStdout.start();
	}

	private void waitForHttpReady() throws IOException, InterruptedException {

		final Thread waitForConnect = new Thread(() -> {
			while (true) {
				try {
					httpUri.toURL().openConnection().connect();
					return;
				} catch (Exception e) {
					//ignore
				}
			}
		});
		waitForConnect.start();
		waitForConnect.join(10_000);
		httpUri.toURL().openConnection().connect();
	}

	@After
	public void stopHttpServer() {

		process.destroy();
		try {
			process.waitFor(1, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			process.destroyForcibly();
		}
	}

	@Override protected Statement withBeforeClasses(Statement statement) {

		final Statement testClassBefore = super.withBeforeClasses(statement);
		final List<FrameworkMethod> beforeTestClass = new TestClass(RunnerWithHttpServer.class).getAnnotatedMethods(Before.class);
		return new RunBefores(testClassBefore, beforeTestClass, this);
	}

	@Override protected Statement withAfterClasses(Statement statement) {

		final List<FrameworkMethod> afterTestClass = new TestClass(RunnerWithHttpServer.class).getAnnotatedMethods(After.class);
		final RunAfters runnerAfterClass = new RunAfters(statement, afterTestClass, this);
		return super.withAfterClasses(runnerAfterClass);
	}

}
