package org.gbif.occurrence.cli.delete.service;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

@MetaInfServices(Command.class)
public class DeleterCommand extends ServiceCommand {

  private final DeleterConfiguration configuration = new DeleterConfiguration();

  public DeleterCommand() {
    super("occurrence-deleter");
  }

  @Override
  protected Service getService() {
    return new DeleterService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

}
