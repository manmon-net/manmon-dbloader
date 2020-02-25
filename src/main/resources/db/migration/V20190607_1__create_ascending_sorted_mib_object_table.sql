CREATE TABLE monitoring_query_mib_objects_ascending(
  query_id BIGINT NOT NULL REFERENCES monitoring_query(id),
  mib_object_id BIGINT NOT NULL
);
