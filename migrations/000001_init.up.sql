-- Таблица метаданных обработки запросов
CREATE TABLE IF NOT EXISTS flight_meta (
                                           id              BIGSERIAL PRIMARY KEY,
                                           flight_number   TEXT        NOT NULL,
                                           departure_date  TIMESTAMPTZ NOT NULL,
                                           status          TEXT        NOT NULL CHECK (status IN ('pending', 'processed', 'error')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ NULL
    );

-- Индекс для быстрого поиска меты по номеру рейса (GET /api/flights/{flight_number}/meta)
CREATE INDEX IF NOT EXISTS idx_flight_meta_flight_number
    ON flight_meta (flight_number);

-- Индекс для фильтрации по номеру + дате (частый кейс обработки)
CREATE INDEX IF NOT EXISTS idx_flight_meta_flight_number_departure
    ON flight_meta (flight_number, departure_date);


-- Основная таблица актуальных данных по рейсам
CREATE TABLE IF NOT EXISTS flights (
                                       flight_number     TEXT        NOT NULL,
                                       departure_date    TIMESTAMPTZ NOT NULL,
                                       aircraft_type     TEXT        NOT NULL,
                                       arrival_date      TIMESTAMPTZ NOT NULL,
                                       passengers_count  INTEGER     NOT NULL CHECK (passengers_count >= 0),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT flights_pk PRIMARY KEY (flight_number, departure_date)
    );

-- Доп. индекс под выборки только по flight_number
CREATE INDEX IF NOT EXISTS idx_flights_flight_number
    ON flights (flight_number);
