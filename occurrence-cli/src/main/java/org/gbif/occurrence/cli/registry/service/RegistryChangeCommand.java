package org.gbif.occurrence.cli.registry.service;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

@MetaInfServices(Command.class)
public class RegistryChangeCommand extends ServiceCommand {

  private final RegistryChangeConfiguration configuration = new RegistryChangeConfiguration();

  public RegistryChangeCommand() {
    super("registry-change-listener");
  }

  @Override
  protected Service getService() {
    return new RegistryChangeService(configuration);
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }
}
