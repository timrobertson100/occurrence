package org.gbif.occurrence.common.download;

import static junit.framework.TestCase.assertEquals;

import org.junit.Test;

/**
 * Simple test for DownloadUtils.
 */
public class DownloadUtilsTest {

  private char NUL_CHAR = '\0';

  @Test
  public void testNUllChar() {
    String testStr = "test";
    assertEquals(testStr, DownloadUtils.DELIMETERS_MATCH_PATTERN.matcher(testStr + NUL_CHAR).replaceAll(""));
  }
}
