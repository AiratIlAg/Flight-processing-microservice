-- Outbox table for Transactional Outbox pattern

-- Для gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS outbox_messages (
    id          BIGSERIAL PRIMARY KEY,
    message_id  UUID        NOT NULL DEFAULT gen_random_uuid(),
    topic       TEXT        NOT NULL,
    payload     JSONB       NOT NULL,

    status      TEXT        NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'sent', 'failed')),
    retry_count INTEGER     NOT NULL DEFAULT 0,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at     TIMESTAMPTZ NULL,
    last_error  TEXT        NULL
    );

-- Индексы по ТЗ
CREATE INDEX IF NOT EXISTS idx_outbox_messages_status
    ON outbox_messages (status);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_created_at
    ON outbox_messages (created_at);

-- (полезно для выборки pending пачками)
CREATE INDEX IF NOT EXISTS idx_outbox_messages_status_created_at
    ON outbox_messages (status, created_at);