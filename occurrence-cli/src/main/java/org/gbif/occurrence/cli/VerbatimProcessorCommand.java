package org.gbif.occurrence.cli;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

@MetaInfServices(Command.class)
public class VerbatimProcessorCommand extends ServiceCommand {

  private final ProcessorCliConfiguration configuration = new ProcessorCliConfiguration();

  public VerbatimProcessorCommand() {
    super("verbatim-processor");
  }

  @Override
  protected Service getService() {
    return new VerbatimProcessorService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
