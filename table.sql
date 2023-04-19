CREATE TABLE IF NOT EXISTS kinesis_shards
(
    shard_id     text NOT NULL default '',
    in_use       bool NOT NULL default false,
    last_seq    text ,
    last_updated timestamp     default now(),
    expired bool not null default false,
    CONSTRAINT kinesis_shards_pk PRIMARY KEY (shard_id)
);