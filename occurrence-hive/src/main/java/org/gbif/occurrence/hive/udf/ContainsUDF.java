package org.gbif.occurrence.hive.udf;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * A simple UDF for Hive to test if a coordinate is contained in a geometry given as well known
 * text. Any geometry supported by jts is accepted, but its up to the user to make sensible queries.
 * In particular polygons should have a complete exterior ring.
 */
@Description(name = "contains", value = "_FUNC_(geom_wkt, latitude, longitude)")
public class ContainsUDF extends UDF {

  private static final Logger LOG = LoggerFactory.getLogger(ContainsUDF.class);
  private final WKTReader wktReader = new WKTReader();
  private final GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
  private final Map<String, Geometry> geometryCache = Maps.newHashMap();
  private final BooleanWritable isContained = new BooleanWritable();

  public BooleanWritable evaluate(Text geometryAsWKT, Double latitude, Double longitude) {
    isContained.set(false);

    try {
      // sanitize the input
      if (geometryAsWKT == null || latitude == null || longitude == null || latitude > 90 || latitude < -90 || longitude > 180
          || longitude < -180) {
        return isContained;
      }
      String geoWKTStr = geometryAsWKT.toString();
      Geometry geom = geometryCache.get(geoWKTStr);
      if (geom == null) {
        geom = wktReader.read(geoWKTStr);
        geometryCache.put(geoWKTStr, geom);
      }

      // support any geometry - up to the user to make a sensible query
      Geometry point = geometryFactory.createPoint(new Coordinate(longitude, latitude));
      isContained.set(geom.contains(point));

    } catch (ParseException e) {
      LOG.error("Invalid geometry received: {}", geometryAsWKT.toString(), e);
    } catch (Exception e) {
      LOG.error("Error applying UDF", e);
    }

    return isContained;
  }
}
