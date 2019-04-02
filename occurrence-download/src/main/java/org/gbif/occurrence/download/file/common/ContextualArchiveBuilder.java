package org.gbif.occurrence.download.file.common;

import org.gbif.api.model.occurrence.sql.SqlDownloadExportFormat;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.occurrence.download.file.simpleavro.SimpleAvroArchiveBuilder;
import org.gbif.occurrence.download.file.simplecsv.SimpleCsvArchiveBuilder;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;

import static org.gbif.api.model.occurrence.sql.SqlDownloadExportFormat.TSV;

/**
 * Archiving file based on type of export format.
 */
public class ContextualArchiveBuilder {

  /**
   * Executes the archive/zip creation process.
   * The expected parameters are:
   * 0. sourcePath: HDFS path to the directory that contains the data files.
   * 1. targetPath: HDFS path where the resulting file will be copied.
   * 2. downloadKey: occurrence download key.
   * 3. MODE: ModalZipOutputStream.MODE of input files.
   * 4. download-format : Download format
   * 5. sql_header: tab separated SQL header in case of SQL Download
   * 6. export_format : type of export file for SQL Download.
   */
  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    FileSystem sourceFileSystem =
      DownloadFileUtils.getHdfs(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY));
    SqlDownloadExportFormat exportFormat = args[6] == null ? TSV : SqlDownloadExportFormat.valueOf(args[6].trim());

    switch (exportFormat) {
      case TSV:
                SimpleCsvArchiveBuilder.withHeader(args[5]).mergeToZip(sourceFileSystem, sourceFileSystem,
                                                                       args[0], args[1], args[2],
                                                                       ModalZipOutputStream.MODE.valueOf(args[3]));
                break;
      case AVRO:
                SimpleAvroArchiveBuilder.mergeToSingleAvro(sourceFileSystem, sourceFileSystem, args[0], args[1], args[2]);
                break;
    }
  }
}
