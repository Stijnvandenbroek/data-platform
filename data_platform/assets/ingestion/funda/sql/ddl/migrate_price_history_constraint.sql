-- Deduplicate and add UNIQUE constraint to price_history if it doesn't exist yet.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{{ schema }}.price_history'::regclass
          AND contype = 'u'
    ) THEN
        DELETE FROM {{ schema }}.price_history a
        USING {{ schema }}.price_history b
        WHERE a.global_id = b.global_id
          AND a.date IS NOT DISTINCT FROM b.date
          AND a.source IS NOT DISTINCT FROM b.source
          AND a.status IS NOT DISTINCT FROM b.status
          AND a.ingested_at < b.ingested_at;

        ALTER TABLE {{ schema }}.price_history
            ADD UNIQUE (global_id, date, source, status);
    END IF;
END $$;
