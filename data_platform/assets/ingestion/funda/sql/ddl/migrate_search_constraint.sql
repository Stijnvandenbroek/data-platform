-- Deduplicate and add UNIQUE constraint to search_results if it doesn't exist yet.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = '{{ schema }}.search_results'::regclass
          AND contype = 'u'
    ) THEN
        DELETE FROM {{ schema }}.search_results a
        USING {{ schema }}.search_results b
        WHERE a.global_id = b.global_id
          AND a.ingested_at < b.ingested_at;

        ALTER TABLE {{ schema }}.search_results
            ADD UNIQUE (global_id);
    END IF;
END $$;
