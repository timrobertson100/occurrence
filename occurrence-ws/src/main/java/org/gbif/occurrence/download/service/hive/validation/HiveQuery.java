package org.gbif.occurrence.download.service.hive.validation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

/**
 * Hive Query extractors which extracts the method.
 */
class HiveQuery {

  private HiveQuery() {}

  /**
   * Data structure for select fields info in SQL Select statement.
   */
  static class SQLSelectFields {

    private final List<String> fields;
    private final boolean hasFunction;

    SQLSelectFields(@NotNull List<String> fields, @NotNull boolean hasFunction) {
      Objects.requireNonNull(fields);
      this.fields = fields;
      this.hasFunction = hasFunction;
    }

    List<String> fields() {
      return fields;
    }

    boolean hasFunction() {
      return hasFunction;
    }
  }

  /**
   * Extractor functions which extracts SQL Query fragments, once the Query is fired with all the
   * rules.
   */
  public static class Extract<U, T> implements BiFunction<U, Extractor<U, T>, T> {

    private final DownloadsQueryRuleBase rb;

    Extract(DownloadsQueryRuleBase rb) {
      this.rb = rb;
    }

    /**
     * field names from AST node.
     *
     * @param rb  rule base
     * @param sql query
     *
     * @return SelectFields information.
     */
    static SQLSelectFields fieldNames(DownloadsQueryRuleBase rb, String sql) {
      return new HiveQuery.Extract<String, SQLSelectFields>(rb).apply(sql, new FieldsNameExtractor());
    }

    /**
     * where clause from SQL.
     *
     * @param rb  rule base, already fired
     * @param sql query
     *
     * @return where clause
     */
    static String whereClause(DownloadsQueryRuleBase rb, String sql) {
      return new HiveQuery.Extract<String, String>(rb).apply(sql, new SimpleWhereClauseExtractor());
    }

    @Override
    public T apply(U t, Extractor<U, T> u) {
      return u.apply(rb, t);
    }
  }

  abstract static class Extractor<U, T> implements BiFunction<DownloadsQueryRuleBase, U, T> {

    abstract T apply(U u);

    public T apply(DownloadsQueryRuleBase ruleBase, U u) {
      if (ruleBase.context().hasIssues()) throw new IllegalStateException("Rule base has issues cannot execute..");
      Optional.ofNullable(ruleBase.context().ruleBase())
        .ifPresent(rb -> Preconditions.checkArgument(rb.getRulesToFire().size() == rb.context()
          .firedRulesByName()
          .size(), "Please fire all rules in rule base before using extract"));
      return apply(u);
    }
  }

  /**
   * Implementation of {@link SQLSelectFields} extractor.
   */
  static class FieldsNameExtractor extends Extractor<String, SQLSelectFields> {

    private static final String SELECT_DISTINCT_REGEX = "(?i)SELECT\\s+DISTINCT";
    private static final String SELECT_DISTINCT = "SELECT DISTINCT";
    private static final String FROM = "FROM";
    private static final String ANY_WHITE_SPACE = "\\s+";

    private boolean hasFunction = false;

    private final Function<String, Integer> indexOfFrom = sql -> sql.toUpperCase().indexOf(FROM);

    private final Function<String, String> fieldSegmentWithDistinct = sql -> {
      hasFunction = true;
      sql = sql.replaceAll(SELECT_DISTINCT_REGEX, SELECT_DISTINCT).trim();
      return sql.substring(15, indexOfFrom.apply(sql));
    };

    private final Function<String, String> fieldSegmentWithoutDistinct =
      sql -> sql.trim().substring(6, indexOfFrom.apply(sql));

    private final Function<String, Boolean> checkFunction = sql -> sql.contains("(") && sql.contains(")");

    @Override
    public SQLSelectFields apply(String sql) {

      String fieldSegment = Optional.of(sql)
        .filter(q -> q.toUpperCase().matches(SELECT_DISTINCT_REGEX + ".*"))
        .map(fieldSegmentWithDistinct)
        .orElse(fieldSegmentWithoutDistinct.apply(sql));
      List<String> fieldsLabel = splits(fieldSegment, ',');
      List<String> fields = fieldsLabel.stream().map(label -> {
        boolean isFunction = checkFunction.apply(label);
        hasFunction = hasFunction || isFunction;
        String[] splits = label.split(ANY_WHITE_SPACE);
        /*
          if there is a function in the label then return entire label else it is an alias send alias.
         */
        String lastString = splits[splits.length - 1];
        if (isFunction && lastString.contains(")")) return label;
        return lastString;
      }).collect(Collectors.toList());
      return new SQLSelectFields(fields, hasFunction);
    }

    /**
     * splits the string with provided delimiter, and avoids the delimiter between a bracket.
     */
    private List<String> splits(String value, char delimiter) {
      char[] ch = value.toCharArray();
      List<String> listOfSplits = new ArrayList<>();
      StringBuilder currWord = new StringBuilder();
      Deque<Character> stack = new ArrayDeque<>();
      for (int i = 0; i < ch.length; i++) {
        if ((ch[i] == delimiter) && (stack.isEmpty())) {
          listOfSplits.add(currWord.toString());
          currWord = new StringBuilder();
          continue;
        }
        if (ch[i] == '(') stack.push('(');
        if (ch[i] == ')') stack.pop();
        currWord.append(ch[i]);
      }
      listOfSplits.add(currWord.toString());
      return listOfSplits;
    }
  }

  /**
   * Implementation of where clause extractor.
   */
  static class SimpleWhereClauseExtractor extends Extractor<String, String> {

    private static final String TOK_WHERE = "WHERE";
    private static final String TOK_GROUP_BY = "GROUP BY";

    @Override
    public String apply(@NotNull String sql) {

      if (!sql.toUpperCase().contains(TOK_WHERE)) return "";
      int whereIndex = sql.toUpperCase().indexOf(TOK_WHERE);
      if (sql.toUpperCase().contains(TOK_GROUP_BY)) {
        int groupByIndex = sql.toUpperCase().indexOf(TOK_GROUP_BY);
        return sql.substring(whereIndex, groupByIndex).substring(5).trim();
      } else {
        return sql.substring(whereIndex).substring(5).trim();
      }
    }
  }

}
