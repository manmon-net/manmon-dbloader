CREATE TABLE metrics (dataid bigint, key1 varchar(255), key2 bigint, hostid bigint, value bigint);
CREATE FUNCTION get_data(dataids bigint[], ascdataids bigint[], hostids bigint[], paramdt timestamp with time zone, resultlimit integer)
RETURNS SETOF metrics AS $$
DECLARE
 did BIGINT;
 mm metrics;
 not_available_hostid BIGINT;
 BEGIN
 FOR not_available_hostid IN SELECT hostid FROM UNNEST(hostids) AS hostid WHERE hostid NOT IN (SELECT hostid FROM data_min_1 WHERE dt=paramdt AND hostid IN (SELECT UNNEST(hostids)))
 LOOP
   SELECT 1001 AS dataid, null as key1, null as key2, not_available_hostid as hostid, null AS VALUE INTO mm;
   RETURN NEXT mm;
 END LOOP;
 FOR did IN SELECT UNNEST(dataids)
 LOOP
   FOR mm IN SELECT dataid,key1,key2,hostid,value FROM data_min_1 WHERE dt=paramdt AND dataid=did AND hostid IN (SELECT UNNEST(hostids)) ORDER BY value DESC LIMIT resultlimit
   LOOP
     RETURN NEXT mm;
   END LOOP;
 END LOOP;
 FOR did IN SELECT UNNEST(ascdataids)
 LOOP
   FOR mm IN SELECT dataid,key1,key2,hostid,value FROM data_min_1 WHERE dt=paramdt AND dataid=did AND hostid IN (SELECT UNNEST(hostids)) ORDER BY value ASC LIMIT resultlimit
   LOOP
     RETURN NEXT mm;
   END LOOP;
 END LOOP;
 END;
$$ LANGUAGE plpgsql;