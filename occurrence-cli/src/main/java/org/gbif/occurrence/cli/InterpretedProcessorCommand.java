package org.gbif.occurrence.cli;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

@MetaInfServices(Command.class)
public class InterpretedProcessorCommand extends ServiceCommand {

  private final ProcessorCliConfiguration configuration = new ProcessorCliConfiguration();

  public InterpretedProcessorCommand() {
    super("interpreted-processor");
  }

  @Override
  protected Service getService() {
    return new InterpretedProcessorService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
