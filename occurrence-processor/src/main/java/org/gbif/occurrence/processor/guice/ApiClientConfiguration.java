package org.gbif.occurrence.processor.guice;

import java.net.URI;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.apache.http.client.HttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.gbif.occurrence.processor.interpreting.util.ObjectMapperContextResolver;
import org.gbif.utils.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;

public class ApiClientConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(ApiClientConfiguration.class);

  /**
   * The base URL to the GBIF API.
   */
  @Parameter(names = "--api-url")
  @NotNull
  public URI url;

  /**
   * http timeout in milliseconds.
   */
  @Parameter(names = "--api-timeout")
  @Min(10)
  public int timeout = 3000;

  /**
   * maximum allowed parallel http connections.
   */
  @Parameter(names = "--max-connections")
  @Min(10)
  public int maxConnections = 100;

  /**
   * @return a new jersey client using a multithreaded http client
   */
  public WebResource newApiClient() {
    ClientConfig cc = new DefaultClientConfig();
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
    cc.getProperties().put(ClientConfig.PROPERTY_READ_TIMEOUT, timeout);
    cc.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, timeout);
    cc.getClasses().add(JacksonJsonProvider.class);
    // use custom configured object mapper ignoring unknown properties
    cc.getClasses().add(ObjectMapperContextResolver.class);

    HttpClient http = HttpUtil.newMultithreadedClient(timeout, maxConnections, maxConnections);
    ApacheHttpClient4Handler hch = new ApacheHttpClient4Handler(http, null, false);
    Client client = new ApacheHttpClient4(hch, cc);

    LOG.info("Connecting to GBIF API: {}", url);
    return client.resource(url);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("url", url).add("timeout", timeout).add("maxConnections", maxConnections).toString();
  }
}
