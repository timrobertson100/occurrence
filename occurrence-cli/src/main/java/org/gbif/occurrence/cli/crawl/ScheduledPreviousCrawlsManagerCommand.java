package org.gbif.occurrence.cli.crawl;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.gbif.cli.Command;
import org.gbif.cli.service.ServiceCommand;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.cli.common.JsonWriter;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Service;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Commands to manage occurrence record from previous crawls in a scheduled task.
 */
@MetaInfServices(Command.class)
public class ScheduledPreviousCrawlsManagerCommand extends ServiceCommand {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledPreviousCrawlsManagerCommand.class);

  private final PreviousCrawlsManagerConfiguration config = new PreviousCrawlsManagerConfiguration();
  private MessagePublisher messagePublisher;

  public ScheduledPreviousCrawlsManagerCommand() {
    super("scheduled-previous-crawls-manager");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected Service getService() {
    Injector injector = Guice.createInjector(new PreviousCrawlModule(config));
    messagePublisher = injector.getInstance(MessagePublisher.class);
    return new ScheduledPreviousCrawlsManagerService(injector.getInstance(PreviousCrawlsManager.class), this::printReport, config,
        this::cleanup);
  }

  private void cleanup() {
    messagePublisher.close();
  }

  /**
   * Print the report to a file or to the console depending on
   * {@link PreviousCrawlsManagerConfiguration}.
   *
   * @param report
   */
  private void printReport(Object report) {
    try {
      if (StringUtils.isNotBlank(config.reportOutputFilepath)) {
        JsonWriter.objectToJsonFile(config.reportOutputFilepath, report);
      }

      if (config.displayReport) {
        System.out.print(JsonWriter.objectToJsonString(report));
      }
    } catch (IOException e) {
      LOG.error("Failed to write report.", e);
    }
  }

}
