-- ingest_data.sql

CREATE TABLE IF NOT EXISTS entity
(
    entity_id   VARCHAR(255) NOT NULL,
    id_type     VARCHAR(255) NOT NULL,
    scheme      VARCHAR(255) NOT NULL,
    class_value VARCHAR(255),
    PRIMARY KEY (entity_id, id_type, scheme)
);

INSERT INTO entity (entity_id, id_type, scheme, class_value)
VALUES ('A', 'Type1', 'Scheme1', 'Value from DB'),
       ('B', 'Type1', 'Scheme1', 'Value from DB'),
       ('C', 'Type2', 'Scheme2', 'Value from DB'),
       ('D', 'Type3', 'Scheme3', 'Value from DB');