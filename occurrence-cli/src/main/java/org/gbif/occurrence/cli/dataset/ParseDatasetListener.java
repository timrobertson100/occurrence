package org.gbif.occurrence.cli.dataset;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Set;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.ParseDatasetMessage;
import org.gbif.common.messaging.api.messages.ParseFragmentMessage;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseDatasetListener extends AbstractMessageCallback<ParseDatasetMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(ParseDatasetListener.class);

  private final OccurrenceKeyPersistenceService occurrenceKeyService;
  private final MessagePublisher messagePublisher;

  public ParseDatasetListener(OccurrenceKeyPersistenceService occurrenceKeyService, MessagePublisher messagePublisher) {
    this.occurrenceKeyService = checkNotNull(occurrenceKeyService, "occurrenceKeyService can't be null");
    this.messagePublisher = checkNotNull(messagePublisher, "messagePublisher can't be null");
  }

  @Override
  public void handleMessage(ParseDatasetMessage message) {
    LOG.info("Parse dataset for key [{}]", message.getDatasetUuid());

    Set<Integer> keys = occurrenceKeyService.findKeysByDataset(message.getDatasetUuid().toString());
    for (Integer key : keys) {
      try {
        messagePublisher.send(new ParseFragmentMessage(key));
      } catch (IOException e) {
        LOG.warn("Could not send ParseFragmentMessage for key [{}] while parsing dataset [{}]", key, message.getDatasetUuid(), e);
      }
    }
  }

}
