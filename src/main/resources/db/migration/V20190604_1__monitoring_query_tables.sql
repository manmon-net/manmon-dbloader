CREATE TABLE monitoring_query(
  id BIGINT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  query_org_id BIGINT NOT NULL,
  private_query BOOLEAN NOT NULL DEFAULT FALSE,
  user_id BIGINT NOT NULL
);
CREATE UNIQUE INDEX monitoring_query_uniq_idx ON monitoring_query(name,query_org_id) WHERE private_query=FALSE;
CREATE UNIQUE INDEX monitoring_query_private_uniq_idx ON monitoring_query(name,user_id) WHERE private_query=TRUE;

CREATE TABLE monitoring_query_hostgroups(
  query_id BIGINT NOT NULL REFERENCES monitoring_query(id),
  hostgroup_id BIGINT NOT NULL
);
CREATE UNIQUE INDEX monitoring_query_hostgroups_uniq_idx ON monitoring_query_hostgroups(query_id,hostgroup_id);

CREATE TABLE monitoring_query_mib_objects (
  query_id BIGINT NOT NULL REFERENCES monitoring_query(id),
  mib_object_id BIGINT NOT NULL
);
CREATE UNIQUE INDEX monitoring_query_mib_objects_uniq_idx ON monitoring_query_mib_objects(query_id,mib_object_id);

CREATE TABLE monitoring_query_hosts (
  query_id BIGINT NOT NULL REFERENCES monitoring_query(id),
  host_id BIGINT NOT NULL
);
CREATE UNIQUE INDEX monitoring_query_hosts_uniq_idx ON monitoring_query_hosts(query_id,host_id);

CREATE TABLE monitoring_query_organizations (
  query_id BIGINT NOT NULL REFERENCES monitoring_query(id),
  org_id BIGINT NOT NULL
);
CREATE UNIQUE INDEX monitoring_query_organizations_uniq_idx ON monitoring_query_organizations(query_id,org_id);
