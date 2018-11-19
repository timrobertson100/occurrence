package org.gbif.occurrence.persistence.hbase;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.annotation.Nullable;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.hbase.util.ResultReader;

/**
 * A convenience class that wraps an HBase table and provides typed get and put operations.
 *
 * @param <T> the type of the HBase table's key
 */
public class HBaseStore<T> {

  private static final String KEY_CANT_BE_NULL_MSG = "key can't be null";
  public static final String HBASE_READ_ERROR_MSG = "Could not read from HBase";

  private final TableName tableName;
  private final String cf;
  private final byte[] cfBytes;
  private final Connection connection;

  // TODO consider a put and get builder that adds columns with successive calls

  public HBaseStore(String tableName, String cf, Connection connection) {
    this.tableName = TableName.valueOf(checkNotNull(tableName, "tableName can't be null"));
    this.cf = checkNotNull(cf, "cf can't be null");
    cfBytes = Bytes.toBytes(cf);
    this.connection = checkNotNull(connection, "connection can't be null");
  }

  public Integer getInt(T key, String columnName) {
    Result row = getRow(key, columnName);
    return ResultReader.getInteger(row, cf, columnName, null);
  }

  public Long getLong(T key, String columnName) {
    Result row = getRow(key, columnName);
    return ResultReader.getLong(row, cf, columnName, null);
  }

  public Double getDouble(T key, String columnName) {
    Result row = getRow(key, columnName);
    return ResultReader.getDouble(row, cf, columnName, null);
  }

  public Float getFloat(T key, String columnName) {
    Result row = getRow(key, columnName);
    return ResultReader.getFloat(row, cf, columnName, null);
  }

  public String getString(T key, String columnName) {
    Result row = getRow(key, columnName);
    return ResultReader.getString(row, cf, columnName, null);
  }

  public byte[] getBytes(T key, String columnName) {
    Result row = getRow(key, columnName);
    return ResultReader.getBytes(row, cf, columnName, null);
  }

  public void putInt(T key, String columnName, int value) {
    put(key, columnName, Bytes.toBytes(value));
  }

  public void putLong(T key, String columnName, long value) {
    put(key, columnName, Bytes.toBytes(value));
  }

  public void putFloat(T key, String columnName, float value) {
    put(key, columnName, Bytes.toBytes(value));
  }

  public void putDouble(T key, String columnName, double value) {
    put(key, columnName, Bytes.toBytes(value));
  }

  public void putString(T key, String columnName, String value) {
    put(key, columnName, Bytes.toBytes(value));
  }

  public long incrementColumnValue(T key, String columnName, long value) {
    checkNotNull(key, KEY_CANT_BE_NULL_MSG);
    checkNotNull(columnName, "columnName can't be null");
    checkNotNull(value, "value can't be null");

    long result = 0;
    try (Table table = connection.getTable(tableName)) {
      byte[] byteKey = convertKey(key);
      if (byteKey != null) {
        result = table.incrementColumnValue(byteKey, cfBytes, Bytes.toBytes(columnName), value);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException(HBASE_READ_ERROR_MSG, e);
    }

    return result;
  }

  private void put(T key, String columnName, byte[] value) {
    checkNotNull(key, KEY_CANT_BE_NULL_MSG);
    checkNotNull(columnName, "columnName can't be null");
    checkNotNull(value, "value can't be null");
    try (Table table = connection.getTable(tableName)) {
      byte[] byteKey = convertKey(key);
      if (byteKey != null) {
        Put put = new Put(byteKey);
        put.addColumn(cfBytes, Bytes.toBytes(columnName), value);
        table.put(put);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException(HBASE_READ_ERROR_MSG, e);
    }
  }

  /**
   * Returns an HBase Result object matching the given key and column name.
   *
   * @param key the primary key of the requested row
   * @param columnName the column value to return
   * @return HBase Result
   *
   * @throws ServiceUnavailableException if there are errors when communicating with HBase
   */
  public Result getRow(T key, String columnName) {
    checkNotNull(key, KEY_CANT_BE_NULL_MSG);
    checkNotNull(columnName, "columnName can't be null");

    Result row = null;
    try (Table table = connection.getTable(tableName)) {
      byte[] byteKey = convertKey(key);
      if (byteKey != null) {
        Get get = new Get(byteKey);
        get.addColumn(cfBytes, Bytes.toBytes(columnName));
        row = table.get(get);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException(HBASE_READ_ERROR_MSG, e);
    }

    return row;
  }

  /**
   * Returns an HBase Result object matching the given key.
   *
   * @param key the primary key of the requested row
   * @return HBase Result
   *
   * @throws ServiceUnavailableException if there are errors when communicating with HBase
   */
  @Nullable
  public Result getRow(T key) {
    checkNotNull(key, KEY_CANT_BE_NULL_MSG);

    Result row = null;
    try (Table table = connection.getTable(tableName)) {
      byte[] byteKey = convertKey(key);
      if (byteKey != null) {
        Get get = new Get(byteKey);
        row = table.get(get);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException(HBASE_READ_ERROR_MSG, e);
    }

    return row;
  }

  /**
   * Do an HBase checkAndPut - a put that will only be attempted if the checkColumn contains the
   * expected checkValue.
   *
   * @param key the primary key of the row
   * @param putColumn the column where the new value will be stored
   * @param putValue the new value to put
   * @param checkColumn the column to check
   * @param checkValue the expected value of the checkColumn
   * @param ts the timestamp to write on the put (if null, the current timestamp will be used)
   * @return true if condition was met and put was successful, false otherwise
   *
   * @throws ServiceUnavailableException if there are errors when communicating with HBase
   */
  public boolean checkAndPut(T key, String putColumn, byte[] putValue, String checkColumn, @Nullable byte[] checkValue, @Nullable Long ts) {
    checkNotNull(key, KEY_CANT_BE_NULL_MSG);
    checkNotNull(putColumn, "putColumn can't be null");
    checkNotNull(putValue, "putValue can't be null");
    checkNotNull(checkColumn, "checkColumn can't be null");

    boolean success = false;
    try (Table table = connection.getTable(tableName)) {
      byte[] byteKey = convertKey(key);
      if (byteKey != null) {
        Put put = new Put(byteKey);
        if (ts != null && ts > 0) {
          put.addColumn(cfBytes, Bytes.toBytes(putColumn), ts, putValue);
        } else {
          put.addColumn(cfBytes, Bytes.toBytes(putColumn), putValue);
        }
        success = table.checkAndPut(byteKey, cfBytes, Bytes.toBytes(checkColumn), checkValue, put);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException(HBASE_READ_ERROR_MSG, e);
    }

    return success;
  }

  // TODO: fix deletions generally and add javadoc
  public void delete(T key, String... columns) {
    checkNotNull(key, KEY_CANT_BE_NULL_MSG);
    checkArgument(columns.length > 0, "columns can't be empty");

    try (Table table = connection.getTable(tableName)) {
      byte[] byteKey = convertKey(key);
      if (byteKey != null) {
        Delete delete = new Delete(byteKey);
        for (String column : columns) {
          delete.addColumn(cfBytes, Bytes.toBytes(column));
        }
        table.delete(delete);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException(HBASE_READ_ERROR_MSG, e);
    }
  }

  private byte[] convertKey(T key) {
    // instanceof is dirty, but it's that or separate classes for different key types
    if (key instanceof Integer) {
      return Bytes.toBytes((Integer) key);
    } else if (key instanceof String) {
      return Bytes.toBytes((String) key);
    } else if (key instanceof Long) {
      return Bytes.toBytes((Long) key);
    } else if (key instanceof Float) {
      return Bytes.toBytes((Float) key);
    } else if (key instanceof Double) {
      return Bytes.toBytes((Double) key);
    }

    return null;
  }
}
