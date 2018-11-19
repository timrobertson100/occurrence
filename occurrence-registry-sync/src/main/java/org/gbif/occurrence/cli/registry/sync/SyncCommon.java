package org.gbif.occurrence.cli.registry.sync;

import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncCommon {

  private static final Logger LOG = LoggerFactory.getLogger(SyncCommon.class);

  public static final String OCC_TABLE_PROPS_KEY = "occurrence.db.table_name";
  public static final String REG_WS_PROPS_KEY = "registry.ws.url";
  public static final String MR_USER_PROPS_KEY = "occurrence.mapreduce.user";
  public static final String PROPS_FILE = "registry-sync.properties";
  public static final String PROPS_FILE_PATH_KEY = "sync.hdfs_config_path";
  public static final byte[] OCC_CF = Columns.CF;
  public static final byte[] DK_COL = Bytes.toBytes(Columns.column(GbifTerm.datasetKey));
  public static final byte[] OOK_COL = Bytes.toBytes(Columns.column(GbifInternalTerm.publishingOrgKey));
  public static final byte[] HC_COL = Bytes.toBytes(Columns.column(GbifTerm.publishingCountry));
  public static final byte[] CI_COL = Bytes.toBytes(Columns.column(GbifInternalTerm.crawlId));
  public static final byte[] LI_COL = Bytes.toBytes(Columns.column(GbifTerm.lastInterpreted));
  public static final byte[] LICENSE_COL = Bytes.toBytes(Columns.column(DcTerm.license));

  public static Properties loadProperties() {
    Properties props = new Properties();
    try (InputStream in = SyncCommon.class.getClassLoader().getResourceAsStream(PROPS_FILE)) {
      props.load(in);
    } catch (Exception e) {
      LOG.error("Unable to open registry-sync.properties file - RegistrySync is not initialized", e);
    }

    return props;
  }
}
