-- Deduplicate and add UNIQUE constraint to listing_details if it doesn't exist yet.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{{ schema }}.listing_details'::regclass
          AND contype = 'u'
    ) THEN
        DELETE FROM {{ schema }}.listing_details a
        USING {{ schema }}.listing_details b
        WHERE a.global_id = b.global_id
          AND a.status IS NOT DISTINCT FROM b.status
          AND a.ingested_at < b.ingested_at;

        ALTER TABLE {{ schema }}.listing_details
            ADD UNIQUE (global_id, status);
    END IF;
END $$;
