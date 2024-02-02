/**
 * Copyright (c) 2017, Stephan Saalfeld All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer. 2. Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.scalableminds.n5.http;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.NonReadableChannelException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5URI;


public class HttpKeyValueAccess implements KeyValueAccess {

  final String baseUri;
  final OkHttpClient httpClient;

  /**
   * Opens an {@link HttpKeyValueAccess} TODO
   *
   * @throws N5Exception.N5IOException if the access could not be created
   */
  public HttpKeyValueAccess(String baseUri) throws N5Exception.N5IOException {
    this.baseUri = baseUri;
    this.httpClient = new OkHttpClient();
  }


  @Override
  public String[] components(final String path) {
    return Arrays.stream(path.split("/"))
        .filter(x -> !x.isEmpty())
        .toArray(String[]::new);
  }

  @Override
  public String compose(final String... components) {
    return normalize(
        Arrays.stream(components)
            .filter(x -> !x.isEmpty())
            .collect(Collectors.joining("/"))
    );

  }

  /**
   * Compose a path from a base uri and subsequent components.
   *
   * @param uri the base path uri
   * @param components the path components
   * @return the path
   */
  @Override
  public String compose(final URI uri, final String... components) {

    final String[] uriComponents = new String[components.length + 1];
    System.arraycopy(components, 0, uriComponents, 1, components.length);
    uriComponents[0] = uri.getPath();
    return compose(uriComponents);
  }

  @Override
  public String parent(final String path) {

    final String[] components = components(path);
    final String[] parentComponents = Arrays.copyOf(components, components.length - 1);

    return compose(parentComponents);
  }

  @Override
  public String relativize(final String path, final String base) {

    try {
      /* Must pass absolute path to `uri`. if it already is, this is redundant, and has no impact on the result.
       * 	It's not true that the inputs are always referencing absolute paths, but it doesn't matter in this
       * 	case, since we only care about the relative portion of `path` to `base`, so the result always
       * 	ignores the absolute prefix anyway. */
      return normalize(uri("/" + base).relativize(uri("/" + path)).toString());
    } catch (final URISyntaxException e) {
      throw new N5Exception("Cannot relativize path (" + path + ") with base (" + base + ")", e);
    }
  }

  @Override
  public String normalize(final String path) {

    return N5URI.normalizeGroupPath(path);
  }

  private String resolve(final String normalPath) {
    return baseUri.replaceAll("\\/+$", "") +"/"+ normalPath;
  }

  @Override
  public URI uri(final String normalPath) throws URISyntaxException {
    return new URI(resolve(normalPath));
  }

  /**
   * Test whether the {@code normalPath} exists.
   * <p>
   * Removes leading slash from {@code normalPath}, and then checks whether
   * either {@code path} or {@code path + "/"} is a key.
   *
   * @param normalPath is expected to be in normalized form, no further
   * 		efforts are made to normalize it.
   * @return {@code true} if {@code path} exists, {@code false} otherwise
   */
  @Override
  public boolean exists(final String normalPath) {
    Request request = new Request.Builder().head().url(resolve(normalPath))        .build();
    Call call = httpClient.newCall(request);
    try (Response response = call.execute()) {
      return response.isSuccessful();
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Test whether the path is a directory.
   * <p>
   * Appends trailing "/" to {@code normalPath} if there is none, removes
   * leading "/", and then checks whether resulting {@code path} is a key.
   *
   * @param normalPath is expected to be in normalized form, no further
   * 		efforts are made to normalize it.
   * @return {@code true} if {@code path} (with trailing "/") exists as a key, {@code false} otherwise
   */
  @Override
  public boolean isDirectory(final String normalPath) {
    return false;
  }

  /**
   * Test whether the path is a file.
   * <p>
   * Checks whether {@code normalPath} has no trailing "/", then removes
   * leading "/" and checks whether the resulting {@code path} is a key.
   *
   * @param normalPath is expected to be in normalized form, no further
   * 		efforts are made to normalize it.
   * @return {@code true} if {@code path} exists as a key and has no trailing slash, {@code false} otherwise
   */
  @Override
  public boolean isFile(final String normalPath) {
    return true;
  }

  @Override
  public LockedChannel lockForReading(final String normalPath) throws IOException {
    return new HttpObjectChannel(resolve(normalPath));
  }

  @Override
  public LockedChannel lockForWriting(final String normalPath) throws IOException {
    throw new RuntimeException("HttpKeyValueAccess is read-only");
  }

  @Override
  public String[] listDirectories(final String normalPath) {
    throw new RuntimeException("HttpKeyValueAccess does not support listing");
  }

  @Override
  public String[] list(final String normalPath) throws IOException {
    throw new RuntimeException("HttpKeyValueAccess does not support listing");
  }

  @Override
  public void createDirectories(final String normalPath) {
    throw new RuntimeException("HttpKeyValueAccess is read-only");
  }

  @Override
  public void delete(final String normalPath) {
    throw new RuntimeException("HttpKeyValueAccess is read-only");
  }

  private class HttpObjectChannel implements LockedChannel {

    protected final String url;
    private final ArrayList<Closeable> resources = new ArrayList<>();

    protected HttpObjectChannel(final String url) {
      this.url = url;
    }

    @Override
    public InputStream newInputStream() throws IOException {
      System.out.println(url);
      Request request = new Request.Builder().get().url(url).build();
      Call call = httpClient.newCall(request);
      Response response = call.execute();
      InputStream inputStream = response.body().byteStream();
      synchronized (resources) {
        resources.add(response);
        resources.add(inputStream);
      }
			return inputStream;
    }

    @Override
    public Reader newReader() throws IOException {
      final InputStreamReader reader = new InputStreamReader(newInputStream(),
          StandardCharsets.UTF_8);
      synchronized (resources) {
        resources.add(reader);
      }
      return reader;
    }

    @Override
    public OutputStream newOutputStream() {
      throw new NonReadableChannelException();
    }

    @Override
    public Writer newWriter() {
      throw new NonReadableChannelException();
    }

    @Override
    public void close() throws IOException {

      synchronized (resources) {
				for (final Closeable resource : resources) {
					resource.close();
				}
        resources.clear();
      }
    }
  }
}
