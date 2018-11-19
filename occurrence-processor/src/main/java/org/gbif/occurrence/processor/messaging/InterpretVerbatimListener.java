package org.gbif.occurrence.processor.messaging;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import org.gbif.api.vocabulary.OccurrencePersistenceStatus;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.InterpretVerbatimMessage;
import org.gbif.occurrence.processor.FragmentProcessor;
import org.gbif.occurrence.processor.InterpretedProcessor;

import com.google.inject.Inject;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

public class InterpretVerbatimListener extends AbstractMessageCallback<InterpretVerbatimMessage> {

  private final InterpretedProcessor interpretedProcessor;

  private final Timer processTimer =
      Metrics.newTimer(FragmentProcessor.class, "interp process time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  @Inject
  public InterpretVerbatimListener(InterpretedProcessor interpretedProcessor) {
    checkNotNull(interpretedProcessor, "interpretedProcessor can't be null");
    this.interpretedProcessor = interpretedProcessor;
  }

  @Override
  public void handleMessage(InterpretVerbatimMessage message) {
    final TimerContext context = processTimer.time();
    try {
      interpretedProcessor.buildInterpreted(message.getOccurrenceKey(), OccurrencePersistenceStatus.UPDATED, false, null, null);
    } finally {
      context.stop();
    }
  }
}
