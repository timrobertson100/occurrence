package org.gbif.occurrence.cli.delete;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.gbif.common.messaging.config.MessagingConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class DeleteOccurrenceConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @Parameter(names = "--occurrence-key")
  public Integer key;

  @Parameter(names = "--occurrence-key-file")
  public String keyFileName;
}
