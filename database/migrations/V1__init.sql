CREATE TABLE events
(
    stream_id      uuid      NOT NULL,
    stream_name    text      NOT NULL,
    stream_version bigserial NOT NULL,
    event_id       uuid      NOT NULL,
    event_name     text      NOT NULL,
    event_data     jsonb     NOT NULL,
    occurred_at    timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (stream_id, stream_name, stream_version)
);

CREATE TABLE snapshots
(
    stream_id      uuid      NOT NULL,
    stream_name    text      NOT NULL,
    stream_version bigserial NOT NULL,
    snapshot_name  text      NOT NULL,
    snapshot_data  jsonb     NOT NULL,
    updated_at     timestamp NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (stream_id, stream_name)
);