package org.gbif.occurrence.cli.index;

import org.apache.solr.client.solrj.SolrServerException;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Callback handler that processes messages for updates and insertions on the Occurrence Index.
 */
class IndexUpdaterCallback extends AbstractMessageCallback<OccurrenceMutatedMessage>  implements Closeable {


  private static class BatchIndexer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BatchIndexer.class);

    private final Lock lock;
    private final Condition condition;
    private final List<Occurrence> occurrences;
    private final SolrOccurrenceWriter solrOccurrenceWriter;
    private final Duration updateWithin;
    private LocalDate lastUpdate = LocalDate.now();
    private boolean running = true;

    public BatchIndexer(Lock lock, Condition condition, List<Occurrence> occurrences,
                        SolrOccurrenceWriter solrOccurrenceWriter, Duration updateWithin) {
        this.lock = lock;
        this.condition = condition;
        this.occurrences = occurrences;
        this.solrOccurrenceWriter = solrOccurrenceWriter;
        this.updateWithin = updateWithin;
    }

    private void waitBatchOrTimer() {
      try {
        while (notReachedCapacityOrExpired()) {
            condition.await();
        }
      } catch (InterruptedException ex) {
         LOG.info("Batch indexer has been interrupted");
         Thread.currentThread().interrupt(); // Here!
         throw new RuntimeException(ex);
      }
    }

    private void updateBatch() {
      try {
          solrOccurrenceWriter.update(occurrences);
      } catch (SolrServerException | IOException ex) {
        LOG.error("Error indexing a batch of occurrences into Solr", ex);
      } finally {
        occurrences.clear();
        lastUpdate = LocalDate.now();
      }
    }

    boolean notReachedCapacityOrExpired() {
        return occurrences.size() < UPDATE_BATCH_SIZE
                || LocalDate.now().minus(updateWithin).compareTo(lastUpdate) < 0;
    }

    void flush() {
      lock.lock();
      try {
         updateBatch();
      } finally {
        lock.unlock();
      }
    }

    void stop() {
       running = false;
    }

    @Override
    public void run() {
       while (running) {
         lock.lock();
         try {
            waitBatchOrTimer();
            updateBatch();
         } finally {
           lock.lock();
         }
       }
    }
  }

  private static final int UPDATE_BATCH_SIZE = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(IndexUpdaterCallback.class);
  private final Counter messageCount = Metrics.newCounter(getClass(), "messageCount");
  private final Counter newOccurrencesCount = Metrics.newCounter(getClass(), "newIndexedOccurrencesCount");
  private final Counter updatedOccurrencesCount = Metrics.newCounter(getClass(), "updatedIndexedOccurrencesCount");
  private final Counter deletedOccurrencesCount = Metrics.newCounter(getClass(), "deletedIndexedOccurrencesCount");
  private final Timer writeTimer = Metrics.newTimer(getClass(), "occurrenceIndexWrites", TimeUnit.MILLISECONDS,
                                                    TimeUnit.SECONDS);

  private final BatchIndexer batchIndexer;
  private final SolrOccurrenceWriter solrOccurrenceWriter;

  private final List<Occurrence> updateBatch;


  private final ExecutorService indexerExecutorService = Executors.newSingleThreadExecutor();

  private final Lock lock;

  private final Condition batchSizeReached;


  /**
   * Flushes all the updates/creates into Solr.
   */
  private void addOrUpdate(Occurrence occurrence) {
      lock.lock();
      try {
        while (batchIndexer.notReachedCapacityOrExpired()) {
          batchSizeReached.await();
        }
        updateBatch.add(occurrence);
        batchSizeReached.signal();
      } catch (InterruptedException ex) {
         Thread.currentThread().interrupt(); // Here!
         throw new RuntimeException(ex);
      } finally {
        lock.unlock();
      }
  }

  /**
   * Default constructor.
   */
  public IndexUpdaterCallback(SolrOccurrenceWriter solrOccurrenceWriter, int solrUpdateBatchSize,
                              long solrUpdateWithinMs) {

    updateBatch = new ArrayList<>(solrUpdateBatchSize);
    lock = new ReentrantLock();
    batchSizeReached = lock.newCondition();
    this.solrOccurrenceWriter = solrOccurrenceWriter;
    batchIndexer = new BatchIndexer(lock, batchSizeReached, updateBatch, solrOccurrenceWriter, Duration.ofMillis(solrUpdateWithinMs));

  }

  @Override
  public void handleMessage(OccurrenceMutatedMessage message) {
    LOG.debug("Handling [{}] occurrence", message.getStatus());
    messageCount.inc();
    TimerContext context = writeTimer.time();
    try {
      switch (message.getStatus()) {
        case NEW:
          // create occurrence
          addOrUpdate(message.getNewOccurrence());
          newOccurrencesCount.inc();
          break;
        case UPDATED:
          // update occurrence
          addOrUpdate(message.getNewOccurrence());
          updatedOccurrencesCount.inc();
          break;
        case DELETED:
          // delete occurrence
          solrOccurrenceWriter.delete(message.getOldOccurrence());
          deletedOccurrencesCount.inc();
          break;
        case UNCHANGED:
          break;
      }
    } catch (Exception e) {
      LOG.error("Error while updating occurrence index for [{}], error [{}]", message.getStatus(), e);
    } finally {
      context.stop();
    }
  }

  /**
   *  Tries an update and stop the timer.
   */
  @Override
  public void close() {
    indexerExecutorService.shutdownNow();
  }
}
