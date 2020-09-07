CREATE TABLE default.ip_ban (
	`ip_min` IPv4,
	`ip_max` IPv4,
	`net` UInt8,
	`ban_type` Enum8('captcha' = 1, 'ban' = 2, 'exclude' = 3) DEFAULT CAST('captcha', 'Enum8(\'captcha\' = 1, \'ban\' = 2, \'exclude\' = 3)'),
	`date_start` DateTime,
	`date_end` Nullable(DateTime),
	`reason` String
) ENGINE = MergeTree()
ORDER BY ip_min
SETTINGS index_granularity = 8192;

CREATE TABLE cluster_shard1.`.inner.api_path_time_view` (
	`intervalTime` DateTime('Europe/Moscow'),
	`path` String,
	`request_time` Float32
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster/cluster_shard1.api_path_time_view', 'replica-1')
PARTITION BY toYYYYMMDD(intervalTime)
ORDER BY intervalTime
SETTINGS index_granularity = 8192;

CREATE TABLE cluster_shard1.api3_http_request (
	`eventTime` DateTime,
	`apiVersion` LowCardinality(String),
	`requestedApiVersion` LowCardinality(String),
	`applicationId` LowCardinality(String),
	`endpointId` LowCardinality(String),
	`httpStatus` UInt16,
	`httpStatusFamily` UInt8,
	`duration` UInt32 COMMENT 'microseconds',
	`applicationType` LowCardinality(Nullable(String)),
	`applicationVersion` LowCardinality(Nullable(String)),
	`osFamily` LowCardinality(Nullable(String)),
	`osVersion` LowCardinality(Nullable(String)),
	`deviceId` Nullable(String)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster/cluster_shard1.api3_http_request', 'replica-1')
PARTITION BY toYYYYMMDD(eventTime)
ORDER BY eventTime
SETTINGS index_granularity = 8192;

CREATE TABLE cluster_shard1.cron_stat (`start_date` Date DEFAULT toDate(start),
	`start_date_time` DateTime DEFAULT toDateTime(start),
	`uid` String,
	`cron_name` LowCardinality(String),
	`host` LowCardinality(String),
	`start` UInt32,
	`end` UInt32,
	`memory` UInt64,
	`memory_limit` UInt64,
	`crash` UInt8
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster/cluster_shard1.cron_stat', 'replica-1')
PARTITION BY toYYYYMMDD(start_date)
ORDER BY start_date
SETTINGS index_granularity = 8192;

CREATE TABLE cluster_shard1.access (
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
	`reg` Int32,
	`ws` String CODEC(ZSTD(1)),
	`admin` Int32,
	`asid` String CODEC(ZSTD(1)),
	`gzip_ratio` Float32,
	`device_type` Nullable(Enum8('desktop' = 1, 'mobile' = 2, '' = 3)),
	`scheme` Nullable(Enum8('http' = 1, 'https' = 2, '' = 3)),
	`front_server_name` String CODEC(ZSTD(1)),
	`lkey` String,
	`app_id` String CODEC(ZSTD(1)),
	`device_id` String CODEC(ZSTD(1)),
	`ngx_request_id` FixedString(32)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster/cluster_shard1.access', 'replica-21')
PARTITION BY toYYYYMMDD(time_local)
PRIMARY KEY time_local
ORDER BY time_local SETTINGS index_granularity = 8192;

CREATE TABLE cluster_shard1.performance_log (`metric` String,
	`duration` UInt32,
	`origWs` String,
	`project` String,
	`platform` String,
	`routeName` String,
	`browserName` String,
	`browserVersion` String,
	`hydrationStage` Nullable(Enum8('false' = 0, 'true' = 1)),
	`userId` UInt32,
	`userIsApplicant` Nullable(Enum8('false' = 0, 'true' = 1)),
	`userIsHr` Nullable(Enum8('false' = 0, 'true' = 1)),
	`eventDate` Date DEFAULT toDate(requestedAt),
	`requestedAt` UInt32,
	`httpVersion` Nullable(Enum8('HTTP/1.0' = 1, 'HTTP/1.1' = 2, 'HTTP/2.0' = 3))
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster/cluster_shard1.performance_log', 'replica-21')
PARTITION BY toYYYYMMDD(eventDate)
PRIMARY KEY (metric, origWs, userId, project)
ORDER BY (metric, origWs, userId, project, eventDate)
TTL eventDate + toIntervalDay(14)
SETTINGS index_granularity = 8192;

CREATE TABLE cluster_shard1.rtb_ad_click (`id_click` UInt64,
	`id_show` UInt64,
	`id_advert` UInt32,
	`id_sj_user` UInt32,
	`sj_user_type` UInt8,
	`app_type` UInt8,
	`app_token` String,
	`ip` UInt32,
	`rtb_price` UInt16,
	`redirect_url` String,
	`created_at` DateTime('Europe/Moscow')
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster/cluster_shard1.rtb_ad_click', 'replica-1')
PARTITION BY toYYYYMM(created_at)
PRIMARY KEY (id_advert, toDate(created_at))
ORDER BY (id_advert, toDate(created_at))
SETTINGS index_granularity = 8192;

CREATE TABLE cluster_shard1.rtb_ad_show (`id_show` UInt64,
	`id_advert` UInt32,
	`id_sj_user` UInt32,
	`sj_user_type` UInt8,
	`app_type` UInt8,
	`app_token` String,
	`ip` UInt32,
	`first_bid` UInt16,
	`second_bid` UInt16,
	`rtb_min_bid` UInt16,
	`sid` String,
	`rtb_serp_id` String,
	`rtb_serp_position` UInt8,
	`rtb_serp_type` String,
	`created_at` DateTime('Europe/Moscow')
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster/cluster_shard1.rtb_ad_show', 'replica-1')
PARTITION BY toYYYYMM(created_at)
PRIMARY KEY (id_advert, toDate(created_at))
ORDER BY (id_advert, toDate(created_at))
SETTINGS index_granularity = 8192;


CREATE TABLE cluster_shard1.ssr_timing (
	`time` DateTime CODEC(DoubleDelta),
	`domLoading` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`domInteractive` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`domContentLoadedEventStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`domContentLoadedEventEnd` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`domComplete` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`unloadEventStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`unloadEventEnd` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`redirectStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`redirectEnd` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`fetchStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`domainLookupStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`domainLookupEnd` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`connectStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`connectEnd` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`secureConnectionStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`requestStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`responseStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`responseEnd` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`loadEventStart` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`loadEventEnd` UInt32 DEFAULT CAST(0, 'UInt32') CODEC(Delta(4)),
	`url` String CODEC(LZ4HC(0)),
	`http_user_agent` LowCardinality(String) CODEC(LZ4HC(0)),
	`admin` UInt32 DEFAULT CAST(0, 'UInt32') COMMENT 'admin' CODEC(Delta(4)),
	`http_version` Enum8('HTTP/1.0' = 1, 'HTTP/1.1' = 2, 'HTTP/2.0' = 3) DEFAULT CAST('HTTP/1.0', 'Enum8(\'HTTP/1.0\' = 1, \'HTTP/1.1\' = 2, \'HTTP/2.0\' = 3)') COMMENT 'client http version',
	`scheme` Enum8('http' = 1, 'https' = 2) DEFAULT CAST('http', 'Enum8(\'http\' = 1, \'https\' = 2)') COMMENT 'client proto',
	`device_type` Enum8('desktop' = 1, 'mobile' = 2) DEFAULT CAST('desktop', 'Enum8(\'desktop\' = 1, \'mobile\' = 2)') COMMENT 'device type from nginx',
	`site_version` Enum8('unknown' = 0, 'php-backend' = 1, 'old-mobile' = 2, 'node-desktop' = 3, 'node-mobile' = 4) DEFAULT CAST('unknown', 'Enum8(\'unknown\' = 0, \'php-backend\' = 1, \'old-mobile\' = 2, \'node-desktop\' = 3, \'node-mobile\' = 4)') COMMENT 'upstream type',
	`site_release` LowCardinality(String) COMMENT 'release tag',
	`ngx_request_id` FixedString(16) COMMENT 'binary request id' CODEC(LZ4HC(0)),
	`token` FixedString(21) COMMENT 'binary token' CODEC(LZ4HC(0)),
	`remote_addr` UInt32 COMMENT 'ip address',
	`maxmind_lat` Nullable(Float64) COMMENT 'maxmind',
	`maxmind_lon` Nullable(Float64) COMMENT 'maxmind',
	`maxmind_city_id` UInt32 COMMENT 'maxmind',
	`maxmind_city_en` LowCardinality(String) COMMENT 'maxmind',
	`maxmind_country_iso_code` LowCardinality(FixedString(2)) COMMENT 'maxmind',
	`maxmind_city_iso_code` LowCardinality(FixedString(3)) COMMENT 'maxmind'
)
	ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster/cluster_shard1.ssr_timing', 'replica-21')
	PARTITION BY toYYYYMMDD(time) ORDER BY time SETTINGS index_granularity = 8192;

CREATE TABLE default.api3_http_request (
	`eventTime` DateTime,
	`apiVersion` LowCardinality(String),
	`requestedApiVersion` LowCardinality(String),
	`applicationId` LowCardinality(String),
	`endpointId` LowCardinality(String),
	`httpStatus` UInt16,
	`httpStatusFamily` UInt8,
	`duration` UInt32 COMMENT 'microseconds',
	`applicationType` LowCardinality(Nullable(String)),
	`applicationVersion` LowCardinality(Nullable(String)),
	`osFamily` LowCardinality(Nullable(String)),
	`osVersion` LowCardinality(Nullable(String)),
	`deviceId` Nullable(String)
) ENGINE = Distributed('nginx_cluster', '', 'api3_http_request', assumeNotNull(if(length(deviceId) > 1, murmurHash3_64(deviceId), rand())));
