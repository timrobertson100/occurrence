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
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class translates predicates in GBIF download API to equivalent Elastic Search query requests.
 * The json string provided by {@link #getQuery(Predicate)} can be used with _search get requests of ES index to produce downloads.
 */
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
  private static final String ES_KEYWORD_GEO_BBOX = "geo_bounding_box";
  private static final String ES_KEYWORD_MATCH = "match";
  private static final String ES_KEYWORD_WILDCARD = "wildcard";
  private static final String ES_KEYWORD_SHOULD = "should";
  private static final String ES_KEYWORD_MUST_NOT = "must_not";
  private static final String ES_KEYWORD_BOOL = "bool";
  private static final String ES_KEYWORD_QUERY = "query";

  private static final String ES_DEFAULT_QUERY = "?q=*:*";

  private final ObjectNode queryNode = new ObjectMapper().createObjectNode();
  
  private static Map<OccurrenceSearchParameter, String> searchParamToESField =
      new EnumMap<>(OccurrenceSearchParameter.class);

  static {
    for (OccurrenceSearchParameter searchParam : OccurrenceSearchParameter.values()) {
      searchParamToESField.put(searchParam, searchParam.name().toLowerCase().replaceAll("_", ""));
    }
  }

  public static String getESEquivalentSearchField(OccurrenceSearchParameter param) {
    return searchParamToESField.get(param);
  }

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into a
   * json query that can be used as the <em>body</em> for _search request of ES index.
   *
   * @param predicate to translate
   *
   * @return body clause
   */
  public synchronized String getQuery(Predicate predicate) throws QueryBuildingException {
    String esQuery = ES_DEFAULT_QUERY;
    if (predicate != null) {
      visit(predicate, queryNode);
      ObjectNode finalQuery = new ObjectMapper().createObjectNode();
      ObjectNode fullQuery = new ObjectMapper().createObjectNode();
      fullQuery.put(ES_KEYWORD_BOOL, queryNode);
      finalQuery.put(ES_KEYWORD_QUERY, fullQuery);
      esQuery = finalQuery.toString();
    }

    // clear all query parameters for new predicate
    queryNode.removeAll();
    return esQuery;
  }

  /**
   * handle conjunction predicate
   *
   * @param predicate conjunction predicate
   * @param currNode  query node position to append
   */
  public void visit(ConjunctionPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    //if currNode is array then it is part of complex query used with AND , OR, NOT
    if (currNode.isArray()) {
      ((ArrayNode) currNode).add(emptyBoolNode());
      //search empty bool node recently created
      List<JsonNode> collect =
        currNode.findValues(ES_KEYWORD_BOOL).stream().filter((node) -> node.size() == 0).collect(Collectors.toList());
      currNode = collect.get(0);
    }

    if (!currNode.has(ES_KEYWORD_MUST)) {
      ObjectNode tempNode = (ObjectNode) currNode;
      tempNode.put(ES_KEYWORD_MUST, JsonNodeFactory.instance.arrayNode());
    }
    // must query structure is equivalent to AND
    visitCompoundPredicate(predicate, ES_KEYWORD_MUST, currNode.get(ES_KEYWORD_MUST));
  }

  /**
   * handle disjunction predicate
   *
   * @param predicate disjunction predicate
   * @param currNode  query node position to append
   */
  public void visit(DisjunctionPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    //if currNode is array then it is part of complex query used with AND , OR, NOT
    if (currNode.isArray()) {
      ((ArrayNode) currNode).add(emptyBoolNode());
      //search empty bool node recently created
      List<JsonNode> collect =
        currNode.findValues(ES_KEYWORD_BOOL).stream().filter((node) -> node.size() == 0).collect(Collectors.toList());
      currNode = collect.get(0);
    }

    if (!currNode.has(ES_KEYWORD_SHOULD)) {
      ObjectNode tempNode = (ObjectNode) currNode;
      tempNode.put(ES_KEYWORD_SHOULD, JsonNodeFactory.instance.arrayNode());
    }
    // should query structure is equivalent to OR
    visitCompoundPredicate(predicate, ES_KEYWORD_SHOULD, currNode.get(ES_KEYWORD_SHOULD));
  }

  /**
   * handles EqualPredicate
   *
   * @param predicate equalPredicate
   * @param currNode  query node position to append
   */
  public void visit(EqualsPredicate predicate, JsonNode currNode) {
    visitSimplePredicate(predicate, ES_KEYWORD_MATCH, currNode);
  }

  /**
   * handle greater than equals predicate
   *
   * @param predicate gte predicate
   * @param currNode  query node position to append
   */
  public void visit(GreaterThanOrEqualsPredicate predicate, JsonNode currNode) {
    visitRangePredicate(predicate, ES_KEYWORD_GTE, currNode);
  }

  /**
   * handles greater than predicate
   *
   * @param predicate greater than predicate
   * @param currNode  query node position to append
   */
  public void visit(GreaterThanPredicate predicate, JsonNode currNode) {
    visitRangePredicate(predicate, ES_KEYWORD_GT, currNode);
  }

  /**
   * handle IN Predicate
   *
   * @param predicate InPredicate
   * @param currNode  query node position to append
   */
  public void visit(InPredicate predicate, JsonNode currNode) {
    handlePredicate(predicate, "", currNode, NodeType.TERMS);
  }

  /**
   * handles less than or equals predicate
   *
   * @param predicate less than or equals
   * @param currNode  query node position to append
   */
  public void visit(LessThanOrEqualsPredicate predicate, JsonNode currNode) {
    visitRangePredicate(predicate, ES_KEYWORD_LTE, currNode);
  }

  /**
   * handles less than predicate
   *
   * @param predicate less than predicate
   * @param currNode  query node position to append
   */
  public void visit(LessThanPredicate predicate, JsonNode currNode) {
    visitRangePredicate(predicate, ES_KEYWORD_LT, currNode);
  }

  /**
   * handles like predicate
   *
   * @param predicate like predicate
   * @param currNode  query node position to append
   */
  public void visit(LikePredicate predicate, JsonNode currNode) {
    visitSimplePredicate(predicate, ES_KEYWORD_WILDCARD, currNode);
  }

  /**
   * handles not predicate
   *
   * @param predicate NOT predicate
   * @param currNode  query node position to append
   */
  public void visit(NotPredicate predicate, JsonNode currNode) throws QueryBuildingException {
    //if currNode is array then it is part of complex query used with AND , OR, NOT
    if (currNode.isArray()) {
      ((ArrayNode) currNode).add(emptyBoolNode());
      //search empty bool node recently created
      List<JsonNode> collect =
        currNode.findValues(ES_KEYWORD_BOOL).stream().filter((node) -> node.size() == 0).collect(Collectors.toList());
      currNode = collect.get(0);
    }

    if (!currNode.has(ES_KEYWORD_MUST_NOT)) {
      ((ObjectNode) currNode).put(ES_KEYWORD_MUST_NOT, JsonNodeFactory.instance.arrayNode());
    }

    visit(predicate.getPredicate(), currNode.get(ES_KEYWORD_MUST_NOT));
  }

  /**
   * handles within predicate
   *
   * @param within   Within predicate
   * @param currNode query node position to append
   */
  public void visit(WithinPredicate within, JsonNode currNode) {
    handlePredicate(within, ES_KEYWORD_GEO_BBOX, currNode, NodeType.GEOBB);
  }

  /**
   * handles ISNOTNULL Predicate
   *
   * @param predicate ISNOTNULL predicate
   * @param currNode  query node position to append
   */
  public void visit(IsNotNullPredicate predicate, JsonNode currNode) {
    handlePredicate(predicate, "", currNode, NodeType.EXIST);
  }

  /**
   * Builds a list of predicates joined by 'op' statements.
   * The final statement will look like this:
   * <p/>
   * <pre>
   * ((predicate) op (predicate) ... op (predicate))
   * </pre>
   */
  private void visitCompoundPredicate(CompoundPredicate predicate, String op, JsonNode currNode)
    throws QueryBuildingException {
    for (Predicate subPredicate : predicate.getPredicates()) {
      visit(subPredicate, currNode);
    }
  }

  /**
   * handles range predicate
   *
   * @param predicate range predicate
   * @param op        can be any of lt2,lt,gte,gt
   * @param currNode  query node position to append
   */
  private void visitRangePredicate(SimplePredicate predicate, String op, JsonNode currNode) {
    handlePredicate(predicate, op, currNode, NodeType.RANGE);
  }

  /**
   * handles simple predicate eg, equals or like type predicate
   *
   * @param predicate simple predicate
   * @param op        wildcard, match
   * @param currNode  query node position to append
   */
  private void visitSimplePredicate(SimplePredicate predicate, String op, JsonNode currNode) {
    handlePredicate(predicate, op, currNode, NodeType.MATCH);
  }

  private void visit(Object object, JsonNode currNode) throws QueryBuildingException {
    Method method;
    try {
      method = getClass().getMethod("visit", object.getClass(), JsonNode.class);
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

  /**
   * handles predicates for both complex and simple conditions and append smaller unit of query to the main query node.
   *
   * @param predicate predicate to handle
   * @param op        case if there are more than one kind of operations eg. in equal predicate (it can be either match or wildcard) or in range case it can be (gte,lte,gt,lt)
   * @param currNode  the currNode used for appending new query node
   * @param type      new query node type to be appended
   */
  private void handlePredicate(Predicate predicate, String op, JsonNode currNode, NodeType type) {
    boolean isComplex = currNode.isArray();

    ArrayNode arrNode = isComplex ? (ArrayNode) currNode : JsonNodeFactory.instance.arrayNode();

    switch (type) {

      case TERMS:
        arrNode.add(getTermsNode((InPredicate) predicate));
        break;
      case RANGE:
        arrNode.add(getRangeNode((SimplePredicate) predicate, op));
        break;
      case EXIST:
        arrNode.add(getExistNode((IsNotNullPredicate) predicate));
        break;
      case MATCH:
        arrNode.add(getEqualNodeWithOp((SimplePredicate) predicate, op));
        break;
      case GEOBB:
        arrNode.add(getBBoxNode(((WithinPredicate) predicate).getGeometry()));
        break;
    }

    if (!isComplex) {
      ((ObjectNode) currNode).put(ES_KEYWORD_MUST, arrNode);
    }
  }

  /**
   * @return emptyBool node
   */
  private JsonNode emptyBoolNode() {
    ObjectNode boolNode = JsonNodeFactory.instance.objectNode();
    boolNode.put(ES_KEYWORD_BOOL, JsonNodeFactory.instance.objectNode());
    return boolNode;
  }

  /**
   * create an terms object eg.
   * {
   * "terms": {
   * "CATALOG_NUMBER": ["value_1", "value_2", "value_3"]
   * }
   * }
   */
  private JsonNode getTermsNode(InPredicate predicate) {
    ObjectNode termsNode = JsonNodeFactory.instance.objectNode();
    ArrayNode valueNode = JsonNodeFactory.instance.arrayNode();
    for (String s : predicate.getValues()) {
      valueNode.add(s);
    }
    termsNode.put(getESEquivalentSearchField(predicate.getKey()), valueNode);

    ObjectNode finalNodeObject = new ObjectMapper().createObjectNode();
    finalNodeObject.put(ES_KEYWORD_TERMS, termsNode);
    return finalNodeObject;
  }

  /**
   * creates an match object eg.
   * {
   * "match": {
   * "INSTITUTION_CODE": "value_2"
   * }
   * }
   *
   * @param op can be wildcard,match etc
   *
   * @return match JSON Node
   */
  private JsonNode getEqualNodeWithOp(SimplePredicate predicate, String op) {
    ObjectNode matchNode = new ObjectMapper().createObjectNode();
    matchNode.put(getESEquivalentSearchField(predicate.getKey()), predicate.getValue());

    ObjectNode finalNodeObject = new ObjectMapper().createObjectNode();
    finalNodeObject.put(op, matchNode);
    return finalNodeObject;
  }

  /**
   * creates an range object eg.
   * {
   * "range": {
   * "MONTH": {
   * "gte": "12"
   * }
   * }
   * }
   *
   * @return range JSON Node
   */
  private JsonNode getRangeNode(SimplePredicate predicate, String op) {
    ObjectNode rangeNode = new ObjectMapper().createObjectNode();
    rangeNode.put(op, predicate.getValue());

    ObjectNode rangeNodeObject = new ObjectMapper().createObjectNode();
    rangeNodeObject.put(getESEquivalentSearchField(predicate.getKey()), rangeNode);

    ObjectNode finalNodeObject = new ObjectMapper().createObjectNode();
    finalNodeObject.put(ES_KEYWORD_RANGE, rangeNodeObject);
    return finalNodeObject;
  }

  /**
   * creates an exist object eg. {
   * "exists": {
   * "field": "CATALOG_NUMBER"
   * }
   * }
   *
   * @return exist JSON Node
   */
  private JsonNode getExistNode(IsNotNullPredicate predicate) {
    ObjectNode existNode = new ObjectMapper().createObjectNode();
    existNode.put(ES_KEYWORD_FIELD, getESEquivalentSearchField(predicate.getParameter()));

    ObjectNode finalNodeObject = new ObjectMapper().createObjectNode();
    finalNodeObject.put(ES_KEYWORD_EXISTS, existNode);
    return finalNodeObject;
  }

  /**
   * creates a boundingbox json node from wkt string eg. {
   * "geo_bounding_box": {
   * "pin.location": {
   * "top_left": {
   * "lat": 10.0,
   * "lon": 10.0
   * },
   * "bottom_right": {
   * "lat": 40.0,
   * "lon": 40.0
   * }
   * }
   * }
   * }
   *
   * @param wkt wkt format for geometry
   */
  private JsonNode getBBoxNode(String wkt) {
    Envelope envelope = parseGeometryParam(wkt);

    ObjectNode pinlocationNode = JsonNodeFactory.instance.objectNode();
    ObjectNode topLeftNode = JsonNodeFactory.instance.objectNode();
    ObjectNode bottomRightNode = JsonNodeFactory.instance.objectNode();
    ObjectNode geoBBoxNode = JsonNodeFactory.instance.objectNode();
    ObjectNode withinQueryNode = JsonNodeFactory.instance.objectNode();

    topLeftNode.put("lat", envelope.getMinX());
    topLeftNode.put("lon", envelope.getMinY());

    bottomRightNode.put("lat", envelope.getMaxX());
    bottomRightNode.put("lon", envelope.getMaxY());

    pinlocationNode.put("top_left", topLeftNode);
    pinlocationNode.put("bottom_right", bottomRightNode);

    geoBBoxNode.put("pin.location", pinlocationNode);

    withinQueryNode.put(ES_KEYWORD_GEO_BBOX, geoBBoxNode);
    return withinQueryNode;
  }

  /**
   * Parses a geometry parameter in WKT format.
   * If the parsed geometry is a rectangle or polygon, the query is transformed into a range query using the southmost and
   * northmost points.
   */
  private Envelope parseGeometryParam(String wkt) {
    try {
      Geometry geometry = new WKTReader().read(wkt);
      if (!geometry.isValid() || geometry.isEmpty()) {
        throw new IllegalArgumentException("Geometry not in a valid WKT format " + wkt);
      }
      //geometry is rectangle or simple
      return geometry.getEnvelopeInternal();

    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /*********************** utility function *****************/

  enum NodeType {
    TERMS, RANGE, EXIST, MATCH, GEOBB
  }
}
