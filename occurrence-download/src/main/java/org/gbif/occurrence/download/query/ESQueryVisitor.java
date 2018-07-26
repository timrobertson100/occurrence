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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;

import com.google.common.base.Throwables;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ESQueryVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(ESQueryVisitor.class);

  private static final String ES_KEYWORD_MUST = "must";
  private static final String ES_KEYWORD_TERMS = "terms";
  private static final String ES_KEYWORD_EXISTS = "exists";
  private static final String ES_KEYWORD_FIELD = "field";
  private static final String ES_KEYWORD_RANGE = "range";
  private static final String ES_KEYWORD_GTE = "gte";
  private static final String ES_KEYWORD_LTE = "lte";
  private static final String ES_KEYWORD_GT = "gt";
  private static final String ES_KEYWORD_LT = "lt";
  private static final String ES_KEYWORD_MATCH = "match";
  private static final String ES_KEYWORD_WILDCARD = "wildcard";
  private static final String ES_KEYWORD_SHOULD = "should";
  private static final String ES_KEYWORD_MUST_NOT = "must_not";
  private static final String ES_KEYWORD_BOOL = "bool";
  private static final String ES_KEYWORD_QUERY = "query";

  private ObjectNode queryNode = new ObjectMapper().createObjectNode();

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into a
   * strings that can be used as the <em>body</em> for _search request of ES index.
   *
   * @param predicate to translate
   *
   * @return body clause
   */
  public String getQuery(Predicate predicate) throws QueryBuildingException {
    String esQuery = "";
    ObjectNode currNode = queryNode;
    if (predicate != null) {
      visit(predicate, currNode);
      ObjectNode finalQuery = new ObjectMapper().createObjectNode();
      ObjectNode fullQuery = new ObjectMapper().createObjectNode();
      fullQuery.put(ES_KEYWORD_BOOL,queryNode);
      finalQuery.put(ES_KEYWORD_QUERY,fullQuery);
      esQuery = finalQuery.toString();
    }

    // clear all query parameters for new predicate
    queryNode.removeAll();
    return esQuery;
  }

  public void visit(ConjunctionPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    //with complex query used with AND , OR, NOT
    if (currNode.isArray()) {
      ((ArrayNode) currNode).add(emptyBoolNode());
      currNode = currNode.findValue(ES_KEYWORD_BOOL);
    }

    if (!currNode.has(ES_KEYWORD_MUST)) {
      ObjectNode tempNode = (ObjectNode) currNode;
      tempNode.put(ES_KEYWORD_MUST, JsonNodeFactory.instance.arrayNode());
    }
    visitCompoundPredicate(predicate, ES_KEYWORD_MUST, currNode.get(ES_KEYWORD_MUST));
  }

  public void visit(DisjunctionPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    //with complex query used with AND , OR, NOT
    if (currNode.isArray()) {
      ((ArrayNode) currNode).add(emptyBoolNode());
      currNode = currNode.findValue(ES_KEYWORD_BOOL);
    }

    if (!currNode.has(ES_KEYWORD_SHOULD)) {
      ObjectNode tempNode = (ObjectNode) currNode;
      tempNode.put(ES_KEYWORD_SHOULD, JsonNodeFactory.instance.arrayNode());
    }
    visitCompoundPredicate(predicate, ES_KEYWORD_SHOULD, currNode.get(ES_KEYWORD_SHOULD));
  }

  /**
   * Supports all parameters incl taxonKey expansion for higher taxa.
   */
  public void visit(EqualsPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    visitSimplePredicate(predicate, ES_KEYWORD_MATCH, currNode);
  }

  public void visit(GreaterThanOrEqualsPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    visitRangePredicate(predicate, ES_KEYWORD_GTE, currNode);
  }

  public void visit(GreaterThanPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    visitRangePredicate(predicate, ES_KEYWORD_GT, currNode);
  }

  public void visit(InPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    //if currNode is an array it is part of other complex predicate else it is simple predicate
    boolean isComplex = currNode.isArray();
    ArrayNode arrNode = isComplex ? (ArrayNode) currNode : JsonNodeFactory.instance.arrayNode();

    arrNode.add(getTermsNode(predicate));
    //since an equal predicate need to be must
    if (!isComplex) {
      ((ObjectNode) currNode).put(ES_KEYWORD_MUST, arrNode);
    }

  }

  public void visit(LessThanOrEqualsPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    visitRangePredicate(predicate, ES_KEYWORD_LTE, currNode);
  }

  public void visit(LessThanPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    visitRangePredicate(predicate, ES_KEYWORD_LT, currNode);
  }

  public void visit(LikePredicate predicate, JsonNode currNode) throws QueryBuildingException {
    visitSimplePredicate(predicate, ES_KEYWORD_WILDCARD, currNode);
  }

  public void visit(NotPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    if (!currNode.has(ES_KEYWORD_MUST_NOT)) {
      ((ObjectNode) currNode).put(ES_KEYWORD_MUST_NOT, JsonNodeFactory.instance.arrayNode());
    }

    visit(predicate.getPredicate(), currNode.get(ES_KEYWORD_MUST_NOT));
  }

  public void visit(WithinPredicate within) {
    /*builder.append(PARAMS_JOINER.join(OccurrenceSolrField.COORDINATE.getFieldName(),
                                      parseGeometryParam(within.getGeometry())));*/
  }

  public void visit(IsNotNullPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    boolean isComplex = currNode.isArray();
    ArrayNode arrNode = isComplex ? (ArrayNode) currNode : JsonNodeFactory.instance.arrayNode();

    arrNode.add(getExistNode(predicate));

    if (!isComplex) {
      ((ObjectNode) currNode).put(ES_KEYWORD_MUST, arrNode);
    }
  }

  /**
   * Builds a list of predicates joined by 'op' statements.
   * The final statement will look like this:
   * <p/>
   * <pre>
   * ((predicate) op (predicate) ... op (predicate))
   * </pre>
   */
  public void visitCompoundPredicate(CompoundPredicate predicate, String op, JsonNode currNode)
    throws QueryBuildingException {
    Iterator<Predicate> iterator = predicate.getPredicates().iterator();
    while (iterator.hasNext()) {
      Predicate subPredicate = iterator.next();
      visit(subPredicate, currNode);
    }
  }

  public void visitRangePredicate(SimplePredicate predicate, String op, JsonNode currNode)
    throws QueryBuildingException {
    boolean isComplex = currNode.isArray();
    ArrayNode arrNode = isComplex ? (ArrayNode) currNode : JsonNodeFactory.instance.arrayNode();

    arrNode.add(getRangeNode(predicate,op));

    if (!isComplex) {
      ((ObjectNode) currNode).put(ES_KEYWORD_MUST, arrNode);
    }
  }



  public void visitSimplePredicate(SimplePredicate predicate, String op, JsonNode currNode)
    throws QueryBuildingException {
    boolean isComplex = currNode.isArray();

    ArrayNode arrNode = isComplex ? (ArrayNode) currNode : JsonNodeFactory.instance.arrayNode();

    arrNode.add(getMatchNode(predicate,op));

    if (!isComplex) {
      ((ObjectNode) currNode).put(ES_KEYWORD_MUST, arrNode);
    }
  }

  private void visit(Object object, JsonNode currNode) throws QueryBuildingException {
    Method method = null;
    try {
      method = getClass().getMethod("visit", new Class[] {object.getClass(), JsonNode.class});
    } catch (NoSuchMethodException e) {
      LOG.warn("Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object, currNode);
    } catch (IllegalAccessException e) {
      LOG.error("This error shouldn't occurr if all visit methods are public. Probably a programming error", e);
      Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the query", e);
      throw new QueryBuildingException(e);
    }
  }

  /*********************** utility function *****************/

  private void handlePredicate(Predicate predicate,String op, JsonNode currNode){

  }

  /**
   *
   * @return emptyBool node
   */
  private JsonNode emptyBoolNode(){
    ObjectNode boolNode = JsonNodeFactory.instance.objectNode();
    boolNode.put(ES_KEYWORD_BOOL, JsonNodeFactory.instance.objectNode());
    return boolNode;
  }

  /**
   * create an terms object eg.
   * {
   * 	"terms": {
   * 		"CATALOG_NUMBER": ["value_1", "value_2", "value_3"]
   *    }
   * }
   * @param predicate
   * @return
   */
  private JsonNode getTermsNode(InPredicate predicate){
    ObjectNode termsNode = JsonNodeFactory.instance.objectNode();
    ArrayNode valueNode = JsonNodeFactory.instance.arrayNode();
    Iterator<String> iterator = predicate.getValues().iterator();
    while (iterator.hasNext()) {
      valueNode.add(iterator.next());
    }
    termsNode.put(predicate.getKey().name(), valueNode);

    ObjectNode finalNodeObject = new ObjectMapper().createObjectNode();
    finalNodeObject.put(ES_KEYWORD_TERMS, termsNode);
    return finalNodeObject;
  }

  /**
   * creates an match object eg.
   * {
   * 	"match": {
   * 		"INSTITUTION_CODE": "value_2"
   *    }
   * }
   * @param predicate
   * @param op can be wildcard,match etc
   * @return match JSON Node
   */
  private JsonNode getMatchNode(SimplePredicate predicate, String op){
    ObjectNode matchNode = new ObjectMapper().createObjectNode();
    matchNode.put(predicate.getKey().name(), predicate.getValue());

    ObjectNode finalNodeObject = new ObjectMapper().createObjectNode();
    finalNodeObject.put(op, matchNode);
    return finalNodeObject;
  }

  /**
   * creates an range object eg.
   * {
   * 	"range": {
   * 		"MONTH": {
   * 			"gte": "12"
   *        }
   *   }
   * }
   * @param predicate
   * @return range JSON Node
   */
  private JsonNode getRangeNode(SimplePredicate predicate,String op){
    ObjectNode rangeNode = new ObjectMapper().createObjectNode();
    rangeNode.put(op, predicate.getValue());

    ObjectNode rangeNodeObject = new ObjectMapper().createObjectNode();
    rangeNodeObject.put(predicate.getKey().name(), rangeNode);

    ObjectNode finalNodeObject = new ObjectMapper().createObjectNode();
    finalNodeObject.put(ES_KEYWORD_RANGE, rangeNodeObject);
    return finalNodeObject;
  }

  /**
   * creates an exist object eg. {
   * 	"exists": {
   * 		"field": "CATALOG_NUMBER"
   *    }
   * }
   * @param predicate
   * @return exist JSON Node
   */
  private JsonNode getExistNode(IsNotNullPredicate predicate){
    ObjectNode existNode = new ObjectMapper().createObjectNode();
    existNode.put(ES_KEYWORD_FIELD, predicate.getParameter().name());

    ObjectNode finalNodeObject = new ObjectMapper().createObjectNode();
    finalNodeObject.put(ES_KEYWORD_EXISTS, existNode);
    return finalNodeObject;
  }
}
