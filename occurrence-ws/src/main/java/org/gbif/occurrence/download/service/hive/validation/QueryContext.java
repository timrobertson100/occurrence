package org.gbif.occurrence.download.service.hive.validation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.gbif.common.shaded.com.google.common.base.Preconditions;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;

/**
 * 
 * Container class of SQL query based context information.
 *
 */
public class QueryContext {

  private final String sql;
  private Issue parseIssue = Issue.NO_ISSUE;
  private SqlSelect selectQueryObject;
  private SqlNode from;
  private List<String> selectFieldNames = new ArrayList<>();
  private Optional<String> where = Optional.empty();
  private Optional<List<String>> groupByFields = Optional.empty();
  private Optional<String> having = Optional.empty();
  private String transSql;
  private boolean hasFunctionsInSelectFields = false;
  
  private static final String OCCURRENCE_TABLE = "occurrence_hdfs";

  private QueryContext(String sql) {
    this.sql = sql;
    try {
      Config config = SqlParser.configBuilder().setCaseSensitive(false).setQuoting(Quoting.BACK_TICK).build();
      this.selectQueryObject = (SqlSelect) SqlParser.create(sql, config).parseQuery();
    } catch (Exception e) {
      parseIssue = Issue.PARSE_FAILED.withComment(e.getMessage());
    }
    update();
  }

  private void update() {
    if (!parseIssue.equals(Issue.NO_ISSUE)) { return; }

    this.from = selectQueryObject.getFrom();
    this.selectFieldNames = selectQueryObject.getSelectList().getList().stream().map(SqlNode::toString).collect(Collectors.toList());
    this.hasFunctionsInSelectFields = selectQueryObject.getSelectList().getList().stream().map(SqlNode::getKind).filter(SqlKind.FUNCTION::contains).count() > 0;
    this.where = selectQueryObject.hasWhere() ? Optional.of(selectQueryObject.getWhere().toString()) : Optional.empty();
    this.groupByFields = Optional.ofNullable(selectQueryObject.getGroup())
        .map(list -> list.getList().stream().map(SqlNode::toString).collect(Collectors.toList()));
    this.having = Optional.ofNullable(selectQueryObject.getHaving()).map(SqlNode::toString);
    this.transSql = selectQueryObject.toSqlString(SqlDialect.DatabaseProduct.HIVE.getDialect()).getSql();
}

  public static QueryContext from(String sql) {
    Objects.requireNonNull(sql);
    Preconditions.checkArgument(!sql.isEmpty());
    return new QueryContext(sql);
  }

  public String sql() {
    return sql;
  }

  public SqlSelect getSelectQueryObject() {
    return selectQueryObject;
  }

  public SqlNode from() {
    return from;
  }

  public List<String> selectFieldNames() {
    return selectFieldNames;
  }

  public Optional<String> where() {
    return where;
  }

  public Optional<List<String>> groupByFields() {
    return groupByFields;
  }

  public Optional<String> having() {
    return having;
  }

  public String translatedQuery() {
    return transSql;
  }

  public Optional<String> tableName() {
    return from.getKind().equals(SqlKind.IDENTIFIER) ? Optional.of(from.toString()) : Optional.empty();
  }

  public boolean hasFunctionsInSelectFields() {
    return hasFunctionsInSelectFields;
  }

  public void ensureTableName() {
    SqlParserPos position = selectQueryObject.getFrom().getParserPosition();
    selectQueryObject.setFrom(new SqlIdentifier(OCCURRENCE_TABLE, position));
    update();
  }

  public boolean hasParseIssue() {
    return parseIssue.equals(Issue.PARSE_FAILED);
  }

  public QueryContext onParseFail(Action action) {
    if (hasParseIssue())
      action.apply(parseIssue);
    return this;
  }

}
