package org.gbif.occurrence.cli.index;

import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.kohsuke.MetaInfServices;

import com.google.common.util.concurrent.Service;

/**
 * Command for index updates and insertions processing.
 */
@MetaInfServices(Command.class)
public class UpdateOccurrenceIndexCommand extends ServiceCommand {

  private final IndexingConfiguration configuration = new IndexingConfiguration();

  public UpdateOccurrenceIndexCommand() {
    super("update-occurrence-index");
  }

  @Override
  protected Object getConfigurationObject() {
    return configuration;
  }

  @Override
  protected Service getService() {
    return new IndexUpdaterService(configuration);
  }
}
