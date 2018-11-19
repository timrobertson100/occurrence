package org.gbif.occurrence.cli.crawl;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.gbif.api.model.crawler.FinishReason;
import org.junit.Test;

/**
 * Unit tests related to {@link PreviousCrawlsManagerConfiguration}.
 */
public class PreviousCrawlsManagerServiceTest {

  @Test
  public void testShouldRunAutomaticDeletionFalse() {
    PreviousCrawlsManagerConfiguration config = new PreviousCrawlsManagerConfiguration();
    config.delete = true;
    config.automaticRecordDeletionThreshold = 30;

    PreviousCrawlsManager pcms = new PreviousCrawlsManager(config, null, null, null);
    DatasetRecordCountInfo drci = getDatasetRecordCountInfo();

    drci.setLastCrawlFragmentEmittedCount(100);
    drci.setCrawlInfo(getCrawlInfoList(getCrawlInfo(1, 50), getCrawlInfo(2, 100)));
    assertEquals(150, drci.getRecordCount());
    assertFalse("No automatic deletion. Percentage of records to remove (33) higher than the configured threshold (30).",
        pcms.shouldRunAutomaticDeletion(drci));
  }

  /**
   * Test the typical case for auto-deletion.
   */
  @Test
  public void testShouldRunAutomaticDeletionTrue() {
    PreviousCrawlsManagerConfiguration config = new PreviousCrawlsManagerConfiguration();
    config.delete = true;

    PreviousCrawlsManager pcms = new PreviousCrawlsManager(config, null, null, null);
    DatasetRecordCountInfo drci = getDatasetRecordCountInfo();

    drci.setLastCrawlFragmentEmittedCount(100);
    drci.setCrawlInfo(getCrawlInfoList(getCrawlInfo(1, 18), getCrawlInfo(2, 100)));
    assertEquals(118, drci.getRecordCount());
    assertTrue(pcms.shouldRunAutomaticDeletion(drci));
  }

  /**
   * Generates a DatasetRecordCountInfo with a random datasetKey.
   *
   * @return
   */
  private DatasetRecordCountInfo getDatasetRecordCountInfo() {
    DatasetRecordCountInfo drci = new DatasetRecordCountInfo();
    drci.setDatasetKey(UUID.randomUUID());
    drci.setFinishReason(FinishReason.NORMAL);
    return drci;
  }

  private DatasetCrawlInfo getCrawlInfo(int crawlId, int count) {
    return new DatasetCrawlInfo(crawlId, count);
  }

  private List<DatasetCrawlInfo> getCrawlInfoList(DatasetCrawlInfo... allCrawlInfo) {
    return Arrays.asList(allCrawlInfo);
  }

}
