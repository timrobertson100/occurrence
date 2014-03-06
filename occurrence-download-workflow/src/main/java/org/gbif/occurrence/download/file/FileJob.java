package org.gbif.occurrence.download.file;

import com.google.common.base.Objects;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Holds the job information used by each file writer job/thread.
 * Examples of instances of this class are:
 * - query:*:* from: 200, to: 500, dataFile:occurrence.txt3.
 * - query:collector_name:juan from: 1000, to: 5500, dataFile:occurrence.txt99.
 */
public class FileJob implements Comparable<FileJob> {

  private final String query;

  private final int from;

  private final int to;

  private final String interpretedDataFile;
  private final String verbatimDataFile;


  /**
   * Default constructor.
   */
  public FileJob(int from, int to, String interpretedDataFile, String verbatimDataFile, String query) {
    checkArgument(to >= from, "'to' parameter should be greater than the 'from' argument");
    this.query = query;
    this.from = from;
    this.to = to;
    this.interpretedDataFile = interpretedDataFile;
    this.verbatimDataFile = verbatimDataFile;
  }

  @Override
  public int compareTo(FileJob that) {
    return Ints.compare(this.getFrom(), that.getFrom());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof FileJob)) {
      return false;
    }

    FileJob that = (FileJob) obj;
    return Objects.equal(this.interpretedDataFile, that.interpretedDataFile) && Objects.equal(this.query, that.query)
      && Objects.equal(this.from, that.from) && Objects.equal(this.to, that.to);
  }


  /**
   * Gets the interpreted data file name.
   */
  public String getInterpretedDataFile() {
    return interpretedDataFile;
  }

  /**
   * Gets the interpreted data file name.
   */
  public String getVerbatimDataFile() {
    return verbatimDataFile;
  }


  /**
   * Offset in the complete result set returned by the query.
   * 
   * @return the from
   */
  public int getFrom() {
    return from;
  }

  /**
   * Search query.
   * 
   * @return the query
   */
  public String getQuery() {
    return query;
  }

  /**
   * Number of the last result to be processed.
   * 
   * @return the to
   */
  public int getTo() {
    return to;
  }


  @Override
  public int hashCode() {
    return Objects.hashCode(this.interpretedDataFile, this.query, this.from, this.to);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("from", from).add("to", to)
      .add("interpretedDataFile", interpretedDataFile).add("verbatimDataFile", verbatimDataFile)
      .add("query", query).toString();
  }

}
