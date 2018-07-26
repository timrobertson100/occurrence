package org.gbif.occurrence.download.query;

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
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class ESQueryVisitorTest {

  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 = OccurrenceSearchParameter.INSTITUTION_CODE;
  ESQueryVisitor visitor = new ESQueryVisitor();

  @Test
  public void testEqualsPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate(PARAM, "value");
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"match\":{\"CATALOG_NUMBER\":\"value\"}}]}}}",query);
  }

  @Test
  public void testGreaterThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "222");
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"range\":{\"ELEVATION\":{\"gte\":\"222\"}}}]}}}",query);
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"range\":{\"ELEVATION\":{\"gt\":\"1000\"}}}]}}}",query);
  }

  @Test
  public void testLessThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new LessThanOrEqualsPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"range\":{\"ELEVATION\":{\"lte\":\"1000\"}}}]}}}",query);
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException {
    Predicate p = new LessThanPredicate(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"range\":{\"ELEVATION\":{\"lt\":\"1000\"}}}]}}}",query);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p3 = new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.MONTH, "12");
    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"match\":{\"CATALOG_NUMBER\":\"value_1\"}},{\"match\":{\"INSTITUTION_CODE\":\"value_2\"}},{\"range\":{\"MONTH\":{\"gte\":\"12\"}}}]}}}",query);
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    DisjunctionPredicate p = new DisjunctionPredicate(Lists.newArrayList(p1, p2));
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"should\":[{\"match\":{\"CATALOG_NUMBER\":\"value_1\"}},{\"match\":{\"INSTITUTION_CODE\":\"value_2\"}}]}}}",query);
  }

  @Test
  public void testInPredicate() throws QueryBuildingException {
    Predicate p = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"));
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"terms\":{\"CATALOG_NUMBER\":[\"value_1\",\"value_2\",\"value_3\"]}}]}}}",query);
  }

  @Test
  public void testComplexInPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new InPredicate(PARAM, Lists.newArrayList("value_1", "value_2", "value_3"));
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"match\":{\"CATALOG_NUMBER\":\"value_1\"}},{\"terms\":{\"CATALOG_NUMBER\":[\"value_1\",\"value_2\",\"value_3\"]}},{\"match\":{\"INSTITUTION_CODE\":\"value_2\"}}]}}}",query);
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException {
    Predicate p = new NotPredicate(new EqualsPredicate(PARAM, "value"));
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must_not\":[{\"match\":{\"CATALOG_NUMBER\":\"value\"}}]}}}",query);
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new EqualsPredicate(PARAM2, "value_2");

    ConjunctionPredicate cp = new ConjunctionPredicate(Lists.newArrayList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must_not\":[{\"bool\":{\"must\":[{\"match\":{\"CATALOG_NUMBER\":\"value_1\"}},{\"match\":{\"INSTITUTION_CODE\":\"value_2\"}}]}}]}}}",query);
  }

  @Test
  public void testLikePredicate() throws QueryBuildingException {
    LikePredicate likePredicate = new LikePredicate(PARAM, "value_1*");
    String query = visitor.getQuery(likePredicate);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"wildcard\":{\"CATALOG_NUMBER\":\"value_1*\"}}]}}}",query);
  }

  @Test
  public void testComplexLikePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new LikePredicate(PARAM, "value_1*");
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");
    Predicate p = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"match\":{\"CATALOG_NUMBER\":\"value_1\"}},{\"wildcard\":{\"CATALOG_NUMBER\":\"value_1*\"}},{\"match\":{\"INSTITUTION_CODE\":\"value_2\"}}]}}}",query);
  }

  @Test
  public void testComplexPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate(PARAM, "value_1");
    Predicate p2 = new LikePredicate(PARAM, "value_1*");
    Predicate p3 = new EqualsPredicate(PARAM2, "value_2");
    Predicate pcon = new ConjunctionPredicate(Lists.newArrayList(p1, p2, p3));
    Predicate pdis = new DisjunctionPredicate(Lists.newArrayList(p1,pcon));
    Predicate p = new NotPredicate(pdis);
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must_not\":[{\"bool\":{\"should\":[{\"match\":{\"CATALOG_NUMBER\":\"value_1\"}},{\"bool\":{\"must\":[{\"match\":{\"CATALOG_NUMBER\":\"value_1\"}},{\"wildcard\":{\"CATALOG_NUMBER\":\"value_1*\"}},{\"match\":{\"INSTITUTION_CODE\":\"value_2\"}}]}}]}}]}}}",query);
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate(PARAM);
    String query = visitor.getQuery(p);
    Assert.assertEquals("{\"query\":{\"bool\":{\"must\":[{\"exists\":{\"field\":\"CATALOG_NUMBER\"}}]}}}",query);
  }
}
