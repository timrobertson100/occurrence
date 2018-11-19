package org.gbif.occurrence.cli.registry.service;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.gbif.common.messaging.config.MessagingConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class RegistryChangeConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--registry-ws-url")
  @NotNull
  public String registryWsUrl;

  @Parameter(names = "--registry-change-queue-name")
  @NotNull
  public String registryChangeQueueName;
}
