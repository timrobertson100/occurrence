package org.gbif.occurrence.download.service;

import static org.junit.Assert.assertTrue;

import org.apache.oozie.client.Job;
import org.junit.Test;

public class DownloadRequestServiceImplTest {

  @Test
  public void testStatusMapCompleteness() throws Exception {
    for (Job.Status st : Job.Status.values()) {
      assertTrue(DownloadRequestServiceImpl.STATUSES_MAP.containsKey(st));
    }
  }
}
