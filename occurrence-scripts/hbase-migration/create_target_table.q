-- NOTE THESE ARE OUT OF DATE: the interpreted fields don't match hbase anymore
DROP TABLE IF EXISTS target_migration_occurrence_tmp;
CREATE EXTERNAL TABLE target_migration_occurrence_tmp(
key INT,
institution_code STRING,
collection_code STRING,
catalogue_number STRING,
scientific_name STRING,
author STRING,
rank STRING,
kingdom STRING,
phylum STRING,
class_rank STRING,
order_rank STRING,
family STRING,
genus STRING,
species STRING,
subspecies STRING,
latitude STRING,
longitude STRING,
lat_lng_precision STRING,
max_altitude STRING,
min_altitude STRING,
min_depth STRING,
max_depth STRING,
continent_ocean STRING,
state_province STRING,
county STRING,
country STRING,
collector_name STRING,
locality STRING,
year STRING,
month STRING,
day STRING,
event_date STRING,
basis_of_record STRING,
identifier_name STRING,
identification_date STRING,
unit_qualifier STRING,
created BIGINT,
last_crawled BIGINT,
i_kingdom STRING,
i_phylum STRING,
i_class STRING,
i_order STRING,
i_family STRING,
i_genus STRING,
i_species STRING,
i_scientific_name STRING,
i_kingdom_key INT,
i_phylum_key INT,
i_class_key INT,
i_order_key INT,
i_family_key INT,
i_genus_key INT,
i_species_key INT,
i_taxon_key INT,
i_country STRING,
i_latitude FLOAT,
i_longitude FLOAT,
i_year INT,
i_month INT,
i_event_date BIGINT,
i_basis_of_record STRING,
iss_COUNTRY_DERIVED_FROM_COORDINATES INT,
i_altitude INT,
i_depth INT,
last_interpreted BIGINT,
dataset_key STRING,
xml STRING,
xml_hash STRING,
xml_schema STRING,
dwc_occurrence_id STRING,
harvested_date BIGINT,
crawl_id INT,
protocol STRING,
pub_country STRING,
pub_org_key STRING,
iss_PRESUMED_NEGATED_LATITUDE SMALLINT,
iss_PRESUMED_NEGATED_LONGITUDE SMALLINT,
iss_PRESUMED_SWAPPED_COORDINATE SMALLINT,
iss_ZERO_COORDINATE SMALLINT,
iss_COORDINATES_OUT_OF_RANGE SMALLINT,
iss_COUNTRY_COORDINATE_MISMATCH SMALLINT)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "
:key,
o:t_dwc_institutionCode,
o:t_dwc_collectionCode,
o:t_dwc_catalogNumber,
o:t_dwc_scientificName,
o:t_dwc_scientificNameAuthorship,
o:t_dwc_taxonRank,
o:t_dwc_kingdom,
o:t_dwc_phylum,
o:t_dwc_class,
o:t_dwc_order,
o:t_dwc_family,
o:t_dwc_genus,
o:t_dwc_specificEpithet,
o:t_dwc_infraspecificEpithet,
o:t_dwc_verbatimLatitude,
o:t_dwc_verbatimLongitude,
o:t_dwc_coordinatePrecision,
o:t_dwc_maximumElevationInMeters,
o:t_dwc_minimumElevationInMeters,
o:t_dwc_minimumDepthInMeters,
o:t_dwc_maximumDepthInMeters,
o:t_dwc_continent,
o:t_dwc_stateProvince,
o:t_dwc_county,
o:t_dwc_country,
o:t_dwc_recordedBy,
o:t_dwc_locality,
o:t_dwc_year,
o:t_dwc_month,
o:t_dwc_day,
o:t_dwc_eventDate,
o:t_dwc_basisOfRecord,
o:t_dwc_identifiedBy,
o:t_dwc_dateIdentified,
o:t_gbif_unitQualifier,
o:crtd,
o:lc,
o:ik,
o:ip,
o:icl,
o:io,
o:if,
o:ig,
o:is,
o:isn,
o:ikk,
o:ipk,
o:ick,
o:iok,
o:ifk,
o:igk,
o:isk,
o:itk,
o:icc,
o:ilat,
o:ilng,
o:iy,
o:im,
o:ied,
o:ibor,
o:iss_COUNTRY_DERIVED_FROM_COORDINATES,
o:ialt,
o:idep,
o:li,
o:dk,
o:x,
o:xh,
o:xs,
o:t_dwc_occurrenceID,
o:hd,
o:ci,
o:pr,
o:pc,
o:pok,
o:iss_PRESUMED_NEGATED_LATITUDE,
o:iss_PRESUMED_NEGATED_LONGITUDE,
o:iss_PRESUMED_SWAPPED_COORDINATE,
o:iss_ZERO_COORDINATE,
o:iss_COORDINATES_OUT_OF_RANGE,
o:iss_COUNTRY_COORDINATE_MISMATCH

")
TBLPROPERTIES ("hbase.table.name" = "${hbase_target_table}", "hbase.table.default.storage.type" = "binary");
