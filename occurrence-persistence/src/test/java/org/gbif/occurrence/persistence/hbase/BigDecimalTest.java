package org.gbif.occurrence.persistence.hbase;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.utils.number.BigDecimalUtils;
import org.junit.Test;

/**
 * Simple test to check the size on {@link BigDecimal} in bytes using HBase {@link Bytes} class.
 */
public class BigDecimalTest {

  @Test
  public void testRoundTrip() {

    double d = 3.23456;
    BigDecimal bigDecimal = BigDecimalUtils.fromDouble(d, true);
    byte[] barr = Bytes.toBytes(bigDecimal);

    BigDecimal bigDecimalRebuilt = Bytes.toBigDecimal(barr);
    assertEquals(bigDecimal, bigDecimalRebuilt);
  }

  @Test
  public void testBigDecimalWithHBaseBytes() {
    double value = 2.5444444d;
    // rounded BigDecimal takes less byte of storage
    BigDecimal number = BigDecimalUtils.fromDouble(value, false);
    BigDecimal numberRounded = number.setScale(1, BigDecimal.ROUND_HALF_UP);

    // Bytes.toBytes(numberRounded).length == 5
    // Bytes.toBytes(number).length == 8
    assertTrue(Bytes.toBytes(numberRounded).length < Bytes.toBytes(number).length);

    // rounded BigDecimal takes less byte of storage than double
    assertTrue(Bytes.toBytes(numberRounded).length < Bytes.toBytes(value).length);
  }
}
