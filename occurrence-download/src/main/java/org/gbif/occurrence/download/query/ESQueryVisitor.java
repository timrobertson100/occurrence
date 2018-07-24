package org.gbif.occurrence.download.query;

import org.gbif.api.model.occurrence.predicate.CompoundPredicate;
import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.SimplePredicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.common.search.solr.SolrConstants;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Optional;

import com.google.common.base.Throwables;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.common.search.solr.QueryUtils.PARAMS_JOINER;

public class ESQueryVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(ESQueryVisitor.class);
  private static final String CONJUNCTION_OPERATOR = " AND ";
  private static final String DISJUNCTION_OPERATOR = " OR ";
  private static final String EQUALS_OPERATOR = ":";
  private static final String GREATER_THAN_OPERATOR = "{%s TO *]";
  private static final String GREATER_THAN_EQUALS_OPERATOR = "[%s TO *]";
  private static final String LESS_THAN_OPERATOR = "[* TO %s}";
  private static final String LESS_THAN_EQUALS_OPERATOR = "[* TO %s]";
  private static final String NOT_OPERATOR = "*:* NOT ";
  private static final String NOT_NULL_COMPARISON = ":*";

  private static final String ES_KEYWORD_MUST = " must ";
  private static final String ES_KEYWORD_RANGE = " range ";
  private static final String ES_KEYWORD_GTE = " gte ";
  private static final String ES_KEYWORD_LTE = " lte ";
  private static final String ES_KEYWORD_GT = " gt ";
  private static final String ES_KEYWORD_LT = " lt ";
  private static final String ES_KEYWORD_MATCH = " match ";
  private static final String ES_KEYWORD_SHOULD = " should ";
  private static final String ES_KEYWORD_MUST_NOT = " must_not ";
  private static final String ES_KEYWORD_FILTER = " filter ";
  private static final String ES_KEYWORD_BOOL = " bool ";
  private static final String ES_KEYWORD_QUERY = " query ";

  private ObjectNode queryNode = new ObjectMapper().createObjectNode();

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into a
   * strings that can be used as the <em>WHERE</em> clause for a Hive download.
   *
   * @param predicate to translate
   *
   * @return WHERE clause
   */
  public String getESQuery(Predicate predicate) throws QueryBuildingException {
    String esQuery = "";
    ObjectNode currNode = queryNode;
    if (predicate != null) {
      visit(predicate, currNode);
      ObjectNode finalQuery = new ObjectMapper().createObjectNode();
      finalQuery.put(ES_KEYWORD_QUERY, queryNode);
      esQuery = finalQuery.toString();
    }

    // Set to null to prevent old StringBuilders hanging around in case this class is reused somewhere else
    queryNode.removeAll();
    return esQuery;
  }

  public void visit(ConjunctionPredicate predicate,ObjectNode currNode) throws QueryBuildingException {
    visitCompoundPredicate(predicate, ES_KEYWORD_MUST,currNode);
  }

  public void visit(DisjunctionPredicate predicate,ObjectNode currNode) throws QueryBuildingException {
    visitCompoundPredicate(predicate, ES_KEYWORD_SHOULD,currNode);
  }

  /**
   * Supports all parameters incl taxonKey expansion for higher taxa.
   */
  public void visit(EqualsPredicate predicate,ObjectNode currNode) throws QueryBuildingException {
    visitSimplePredicate(predicate, ES_KEYWORD_MUST, currNode);
  }

  public void visit(GreaterThanOrEqualsPredicate predicate,ObjectNode currNode) throws QueryBuildingException {
    visitRangePredicate(predicate, ES_KEYWORD_GTE,currNode);
  }

  public void visit(GreaterThanPredicate predicate,ObjectNode currNode) throws QueryBuildingException {
    visitRangePredicate(predicate, ES_KEYWORD_GT,currNode);
  }

  public void visit(InPredicate predicate) throws QueryBuildingException {
    /*builder.append('(');
    Iterator<String> iterator = predicate.getValues().iterator();
    while (iterator.hasNext()) {
      String value = iterator.next();
      builder.append('(');
      builder.append(toSolrField(predicate.getKey()));
      builder.append(EQUALS_OPERATOR);
      builder.append(toSolrValue(predicate.getKey(), value));
      builder.append(')');
      if (iterator.hasNext()) {
        builder.append(DISJUNCTION_OPERATOR);
      }
    }
    builder.append(')');*/
  }

  public void visit(LessThanOrEqualsPredicate predicate, ObjectNode currNode) throws QueryBuildingException {
    visitRangePredicate(predicate, ES_KEYWORD_LTE,currNode);
  }

  public void visit(LessThanPredicate predicate, ObjectNode currNode) throws QueryBuildingException {
    visitRangePredicate(predicate, ES_KEYWORD_LT, currNode);
  }

  public void visit(LikePredicate predicate) throws QueryBuildingException {
    /*builder.append(toSolrField(predicate.getKey()));
    builder.append(EQUALS_OPERATOR);
    builder.append(toSolrValue(predicate.getKey(), predicate.getValue() + SolrConstants.DEFAULT_FILTER_QUERY));*/
  }

  // TODO: This probably won't work without a bit more intelligence
  public void visit(NotPredicate predicate) throws QueryBuildingException {
    /*builder.append('(');
    builder.append(NOT_OPERATOR);
    visit(predicate.getPredicate());
    builder.append(')');*/
  }

  public void visit(WithinPredicate within) {
    /*builder.append(PARAMS_JOINER.join(OccurrenceSolrField.COORDINATE.getFieldName(),
                                      parseGeometryParam(within.getGeometry())));*/
  }

  public void visit(IsNotNullPredicate predicate) throws QueryBuildingException {
    /*builder.append(toSolrField(predicate.getParameter()));
    builder.append(NOT_NULL_COMPARISON);*/
  }

  /**
   * Builds a list of predicates joined by 'op' statements.
   * The final statement will look like this:
   * <p/>
   * <pre>
   * ((predicate) op (predicate) ... op (predicate))
   * </pre>
   */
  public void visitCompoundPredicate(CompoundPredicate predicate, String op,ObjectNode currNode) throws QueryBuildingException {
    Iterator<Predicate> iterator = predicate.getPredicates().iterator();
    while (iterator.hasNext()) {
      Predicate subPredicate = iterator.next();
      visit(subPredicate,currNode);
    }
  }

  public void visitRangePredicate(SimplePredicate predicate, String op,ObjectNode currNode) throws QueryBuildingException {
    ArrayNode arrNode =(ArrayNode)Optional.ofNullable(currNode.get(ES_KEYWORD_MUST)).orElse(new ObjectMapper().createArrayNode());
    ObjectNode rangeNode= new ObjectMapper().createObjectNode();
    rangeNode.put(op,predicate.getValue());

    ObjectNode rangeNodeObject = new ObjectMapper().createObjectNode();
    rangeNodeObject.put(predicate.getKey().name(),rangeNode);

    ObjectNode finalNodeObject = new ObjectMapper().createObjectNode();
    finalNodeObject.put(ES_KEYWORD_RANGE,rangeNodeObject);

    arrNode.add(finalNodeObject);
    currNode.put(ES_KEYWORD_MUST,arrNode);
  }

  public void visitSimplePredicate(SimplePredicate predicate, String op,ObjectNode currNode) throws QueryBuildingException {
    ArrayNode arrNode =(ArrayNode)Optional.ofNullable(currNode.get(ES_KEYWORD_MUST)).orElse(new ObjectMapper().createArrayNode());
    ObjectNode matchNode= new ObjectMapper().createObjectNode();
    matchNode.put(predicate.getKey().name(),predicate.getValue());

    ObjectNode matchNodeObject = new ObjectMapper().createObjectNode();
    matchNodeObject.put(ES_KEYWORD_MATCH,matchNode);
    arrNode.add(matchNodeObject);
    currNode.put(ES_KEYWORD_MUST,arrNode);
  }

  private void visit(Object object,ObjectNode currNode) throws QueryBuildingException {
    Method method = null;
    try {
      method = getClass().getMethod("visit", new Class[] {object.getClass(),ObjectNode.class});
    } catch (NoSuchMethodException e) {
      LOG.warn("Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object,currNode);
    } catch (IllegalAccessException e) {
      LOG.error("This error shouldn't occurr if all visit methods are public. Probably a programming error", e);
      Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the query", e);
      throw new QueryBuildingException(e);
    }
  }

}
