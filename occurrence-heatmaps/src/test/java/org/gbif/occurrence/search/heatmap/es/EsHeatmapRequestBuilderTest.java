package org.gbif.occurrence.search.heatmap.es;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.POJONode;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.junit.Test;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.heatmap.es.EsHeatmapRequestBuilder.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EsHeatmapRequestBuilderTest {

  @Test
  public void heatmapRequestTest() {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    ObjectNode json = EsHeatmapRequestBuilder.buildQuery(request);

    assertEquals(0, json.get(SIZE).asInt());

    // aggs
    assertTrue(json.path(AGGS).path(BOX_AGGS).path(FILTER).has(GEO_BOUNDING_BOX));

    // assert bbox
    JsonNode bbox =
        json.path(AGGS)
            .path(BOX_AGGS)
            .path(FILTER)
            .path(GEO_BOUNDING_BOX)
            .path(OccurrenceEsField.COORDINATE_POINT.getFieldName());
    assertEquals("[-44.0, 54.0]", ((POJONode) bbox.path("top_left")).asText());
    assertEquals("[-32.0, 30.0]", ((POJONode) bbox.path("bottom_right")).asText());

    // geohash_grid
    assertTrue(json.path(AGGS).path(BOX_AGGS).path(AGGS).path(HEATMAP_AGGS).has(GEOHASH_GRID));
    JsonNode jsonGeohashGrid =
        json.path(AGGS).path(BOX_AGGS).path(AGGS).path(HEATMAP_AGGS).path(GEOHASH_GRID);
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getFieldName(), jsonGeohashGrid.get(FIELD).asText());
    assertEquals(1, jsonGeohashGrid.get(PRECISION).asInt());

    // geo_bounds
    assertTrue(
        json.path(AGGS)
            .path(BOX_AGGS)
            .path(AGGS)
            .path(HEATMAP_AGGS)
            .path(AGGS)
            .path(CELL_AGGS)
            .has(GEO_BOUNDS));
    JsonNode jsonGeobounds =
        json.path(AGGS)
            .path(BOX_AGGS)
            .path(AGGS)
            .path(HEATMAP_AGGS)
            .path(AGGS)
            .path(CELL_AGGS)
            .path(GEO_BOUNDS);
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getFieldName(), jsonGeobounds.get(FIELD).asText());
  }

  @Test
  public void heatmapRequestFilteredTest() {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.addTaxonKeyFilter(4);
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    ObjectNode json = EsHeatmapRequestBuilder.buildQuery(request);

    assertEquals(0, json.get(SIZE).asInt());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).isArray());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).get(0).has(TERM));

    // taxon key
    assertEquals(
        4,
        json.path(QUERY)
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(TERM)
            .get(OccurrenceEsField.TAXA_KEY.getFieldName())
            .asInt());

    // aggs
    assertTrue(json.path(AGGS).path(BOX_AGGS).path(FILTER).has(GEO_BOUNDING_BOX));

    // geohash_grid
    assertTrue(json.path(AGGS).path(BOX_AGGS).path(AGGS).path(HEATMAP_AGGS).has(GEOHASH_GRID));

    // geo_bounds
    assertTrue(
        json.path(AGGS)
            .path(BOX_AGGS)
            .path(AGGS)
            .path(HEATMAP_AGGS)
            .path(AGGS)
            .path(CELL_AGGS)
            .has(GEO_BOUNDS));
  }
}