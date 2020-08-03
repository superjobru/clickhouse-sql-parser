CREATE TABLE default.access (
    `remote_addr` String CODEC(ZSTD(1)),
    `remote_addr_long` Int32,
    `remote_user` String CODEC(ZSTD(1)),
    `time_local` DateTime CODEC(Delta(1), LZ4HC(0)),
     `id` Int32,
     `request_method` Nullable(Enum8('get' = 1, 'head' = 2, 'post' = 3, 'put' = 4, 'delete' = 5, 'trace' = 6, 'options' = 7, 'connect' = 8, 'purge' = 9, 'patch' = 10, '' = 11)),
     `request` Nullable(String) CODEC(ZSTD(1)),
     `http_version` Nullable(Enum8('HTTP/1.0' = 1, 'HTTP/1.1' = 2, 'HTTP/2.0' = 3, '' = 4)),
     `status` Int32,
     `body_bytes_sent` Int32,
     `http_referer` Nullable(String) CODEC(ZSTD(1)),
     `http_user_agent` String CODEC(ZSTD(1)),
     `http_x_forwarded_for` String CODEC(ZSTD(1)),
     `http_host` String CODEC(ZSTD(1)),
     `request_time` Float32,
     `upstream_response_time` Float32,
     `upstream_cache_status` Nullable(Enum8('MISS' = 1, 'BYPASS' = 2, 'EXPIRED' = 3, 'STALE' = 4, 'UPDATING' = 5, 'REVALIDATED' = 6, 'HIT' = 7, '-' = 8, '' = 9)),
     `upstream_addr` String CODEC(ZSTD(1)),
     `http_x_requested_with` String CODEC(ZSTD(1)),
     `dds` String CODEC(ZSTD(1)),
     `gzip_ratio` Float32,
     `device_type` Nullable(Enum8('desktop' = 1, 'mobile' = 2, '' = 3)),
     `scheme` Nullable(Enum8('http' = 1, 'https' = 2, '' = 3)),
     `front_name` String CODEC(ZSTD(1)),
     `lkey` String,
     `app_id` String CODEC(ZSTD(1)),
     `device_id` String CODEC(ZSTD(1)),
     `ngx_request_id` FixedString(32)
) ENGINE = Distributed('cluster', '', 'access', rand()) ;

CREATE TABLE default.per
(
	`metric` String, 
	`eventDate` Date DEFAULT toDate(requestedAt), 
	`requestedAt` UInt32, 
	`userIsA` Nullable(Enum8('false' = 0, 'true' = 1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(eventDate)
PRIMARY KEY metric
ORDER BY metric
SETTINGS index_granularity = 8192;

CREATE TABLE ttl
(
	`metric` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(eventDate)
PRIMARY KEY (metric,eventDate)
ORDER BY metric
SAMPLE BY intHash32(UserID) 
TTL a + INTERVAL 1 MONTH,
   a + INTERVAL 1 MONTH DELETE,
   b + INTERVAL 1 DAY TO VOLUME 'aaa',
   b + INTERVAL 1 DAY TO DISK 'aaa'
SETTINGS
	index_granularity = 8192,
	storage_policy = 'moving_from_ssd_to_hdd';

