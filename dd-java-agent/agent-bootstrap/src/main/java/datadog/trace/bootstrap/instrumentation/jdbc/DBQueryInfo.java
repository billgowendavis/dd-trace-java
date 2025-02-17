package datadog.trace.bootstrap.instrumentation.jdbc;

import datadog.trace.api.cache.DDCache;
import datadog.trace.api.cache.DDCaches;
import datadog.trace.api.function.Function;
import datadog.trace.api.normalize.SQLNormalizer;
import datadog.trace.bootstrap.instrumentation.api.UTF8BytesString;

public final class DBQueryInfo {

  private static final DDCache<String, DBQueryInfo> CACHED_PREPARED_STATEMENTS =
      DDCaches.newFixedSizeCache(512);
  private static final Function<String, DBQueryInfo> NORMALIZE =
      new Function<String, DBQueryInfo>() {

        @Override
        public DBQueryInfo apply(String sql) {
          return new DBQueryInfo(sql);
        }
      };

  public static DBQueryInfo ofStatement(String sql) {
    return new DBQueryInfo(sql);
  }

  public static DBQueryInfo ofPreparedStatement(String sql) {
    return CACHED_PREPARED_STATEMENTS.computeIfAbsent(sql, NORMALIZE);
  }

  private final UTF8BytesString operation;
  private final UTF8BytesString sql;

  public DBQueryInfo(String sql) {
    this.sql = SQLNormalizer.normalize(sql);
    this.operation = UTF8BytesString.create(extractOperation(this.sql));
  }

  public UTF8BytesString getOperation() {
    return operation;
  }

  public UTF8BytesString getSql() {
    return sql;
  }

  public static CharSequence extractOperation(CharSequence sql) {
    if (null == sql) {
      return null;
    }
    int start = 0;
    for (int i = 0; i < sql.length(); ++i) {
      if (Character.isAlphabetic(sql.charAt(i))) {
        start = i;
        break;
      }
    }
    int firstWhitespace = -1;
    for (int i = start; i < sql.length(); ++i) {
      char c = sql.charAt(i);
      if (Character.isWhitespace(c)) {
        firstWhitespace = i;
        break;
      }
    }
    if (firstWhitespace > -1) {
      return sql.subSequence(start, firstWhitespace);
    }
    return null;
  }
}
