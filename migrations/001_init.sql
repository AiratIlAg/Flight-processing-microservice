CREATE TABLE IF NOT EXISTS flight_meta (
    id BIGSERIAL PRIMARY KEY,
    flight_number   TEXT        NOT NULL,
    departure_date  TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ
    );

CREATE INDEX idx_meta_flight_number
    ON flight_meta(flight_number);

CREATE INDEX idx_meta_status
    ON flight_meta(status);

CREATE TABLE IF NOT EXISTS flights (
    flight_number    TEXT        NOT NULL,
    departure_date   TIMESTAMPTZ NOT NULL,
    aircraft_type    TEXT        NOT NULL,
    arrival_date     TIMESTAMPTZ NOT NULL,
    passengers_count INTEGER     NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT flights_pk
    PRIMARY KEY (flight_number, departure_date)
    );


CREATE INDEX idx_flights_updated
    ON flights(updated_at);