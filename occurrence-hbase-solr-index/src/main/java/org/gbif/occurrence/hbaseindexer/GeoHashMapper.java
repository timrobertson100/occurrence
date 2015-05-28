package org.gbif.occurrence.hbaseindexer;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.io.GeohashUtils;

public class GeoHashMapper {


  public static void main(String[] args){
    System.out.println(GeohashUtils.encodeLatLon(10, 20, 1));
    System.out.println(GeohashUtils.decode(GeohashUtils. encodeLatLon(10,20,1), SpatialContext.GEO));

    System.out.println(GeohashUtils.encodeLatLon(10,20,2));
    System.out.println(GeohashUtils.decode(GeohashUtils. encodeLatLon(10,20,2), SpatialContext.GEO));

    System.out.println(GeohashUtils.encodeLatLon(10,20,3));
    System.out.println(GeohashUtils.decode(GeohashUtils. encodeLatLon(10,20,3), SpatialContext.GEO));

    System.out.println(GeohashUtils.encodeLatLon(10,20,4));
    System.out.println(GeohashUtils.decode(GeohashUtils. encodeLatLon(10,20,4), SpatialContext.GEO));


    System.out.println(GeohashUtils.encodeLatLon(10,30,1));
    System.out.println(GeohashUtils.encodeLatLon(10,30,2));
    System.out.println(GeohashUtils.encodeLatLon(10,30,3));
    System.out.println(GeohashUtils.encodeLatLon(10,30,4));
    System.out.println(GeohashUtils.encodeLatLon(10,30,5));
    System.out.println(GeohashUtils.encodeLatLon(10,30,6));
  }
}
