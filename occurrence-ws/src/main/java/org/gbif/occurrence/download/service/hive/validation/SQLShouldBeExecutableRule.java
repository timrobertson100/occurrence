package org.gbif.occurrence.download.service.hive.validation;

import java.util.Collections;
import java.util.List;
import org.gbif.occurrence.download.service.hive.HiveSQL;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Rule checks if the provided query is executable else through {@linkplain Query.Issue}
 *
 */
public class SQLShouldBeExecutableRule implements Rule {
  private static final String ERROR_EXECUTING_MSG = "Error occurred while executing EXPLAIN. ";
  private static final Logger LOG = LoggerFactory.getLogger(SQLShouldBeExecutableRule.class);
  public static final String COMPILATION_ERROR = "COMPILATION ERROR";
  private List<String> explain;

  public List<String> explainValue() {
    return explain;
  }

  private List<String> explain(String sql) {
    return HiveSQL.Execute.explain(sql);
  }

  @Override
  public RuleContext apply(QueryContext context) {
    try {
      context.ensureTableName();
      explain = explain(context.translatedQuery());
    } catch (RuntimeException e) {
      LOG.error(ERROR_EXECUTING_MSG, e);
      explain = Collections.singletonList(COMPILATION_ERROR);
      return Rule.violated(Issue.CANNOT_EXECUTE.withComment(e.getMessage()));
    }
    return Rule.preserved();
  }

}
