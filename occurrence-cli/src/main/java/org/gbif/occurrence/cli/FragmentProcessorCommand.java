package org.gbif.occurrence.cli;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

@MetaInfServices(Command.class)
public class FragmentProcessorCommand extends ServiceCommand {

  private final ProcessorCliConfiguration configuration = new ProcessorCliConfiguration();

  public FragmentProcessorCommand() {
    super("fragment-processor");
  }

  @Override
  protected Service getService() {
    return new FragmentProcessorService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
