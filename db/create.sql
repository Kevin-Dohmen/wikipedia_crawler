-- +----------------------+
-- | URLs                 |
-- +----------------------+

CREATE TABLE IF NOT EXISTS found_urls (
    id     BIGSERIAL PRIMARY KEY,
    url    TEXT      NOT NULL CHECK (char_length(url) <= 4096),
    status BOOLEAN   NULL,
    error  BOOLEAN   NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS found_urls_url_hidx ON found_urls USING hash(url);

CREATE TABLE IF NOT EXISTS url_relations (
    referencing_url BIGINT NOT NULL REFERENCES found_urls(id) ON DELETE CASCADE,
    referenced_url  BIGINT NOT NULL REFERENCES found_urls(id) ON DELETE CASCADE,
    PRIMARY KEY (referencing_url, referenced_url)
);
