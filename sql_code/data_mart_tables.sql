CREATE TABLE dim_language (
  lang_id serial PRIMARY KEY,
  default_audio_language VARCHAR(10) NULL
);

CREATE TABLE dim_video (
  video_id VARCHAR(255) PRIMARY KEY NOT NULL,
  video_title VARCHAR(255) NOT NULL,
  video_description TEXT NULL,
  lang_id INT NOT NULL,
  FOREIGN KEY (lang_id) REFERENCES dim_language (lang_id),
  video_duration INTERVAL NOT NULL,
  tags TEXT[] NULL
);

CREATE TABLE dim_date (
  date_id serial PRIMARY KEY,
  "date" date NOT NULL,
  "year" int4 NOT NULL,
  "month" int4 NOT NULL,
  "day" int4 NOT NULL
);

ALTER TABLE dim_date
ADD CONSTRAINT unique_date_key UNIQUE (date_id);

CREATE TABLE dim_query (
  query_id serial PRIMARY KEY,
  query_text TEXT NOT NULL
);

CREATE TABLE dim_channel (
  channel_id VARCHAR(255) PRIMARY KEY NOT NULL,
  channel_title VARCHAR(255),
  channel_description TEXT
);

CREATE TABLE fact_channel_stat (
  channel_id VARCHAR(255) NOT NULL,
  FOREIGN KEY (channel_id) REFERENCES dim_channel (channel_id),
  date_id int4 NOT NULL DEFAULT 0,
  FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
  subscriber_count INT,
  video_count INT,
  created_at DEFAULT current_date
);

CREATE TABLE fact_video_stat (
  video_id VARCHAR(255) NOT NULL,
  FOREIGN KEY (video_id) REFERENCES dim_video (video_id),
  channel_id VARCHAR(255) NOT NULL,
  FOREIGN KEY (channel_id) REFERENCES dim_channel (channel_id),
  date_id INT NOT NULL,
  FOREIGN KEY (date_id) REFERENCES dim_date (date_id),
  ----
  viewCount INT NOT NULL,
  likeCount INT NOT NULL,
  commentCount INT NULL,
  created_at date DEFAULT current_date
);

CREATE TABLE dim_video_query (
  video_id VARCHAR(255) NOT NULL,
  FOREIGN KEY (video_id) REFERENCES dim_video (video_id),
  query_id int4 NOT NULL,
  FOREIGN KEY (query_id) REFERENCES dim_query (query_id)
);

CREATE TABLE dag_run (
  id serial PRIMARY KEY,
  start_date timestamp NOT null
);





