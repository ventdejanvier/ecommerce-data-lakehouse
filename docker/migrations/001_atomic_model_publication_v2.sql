-- This file is executed transactionally by model_publication_migration.py.
-- Do not run it through PostgreSQL startup initialization for an existing volume.

CREATE TABLE IF NOT EXISTS public.model_publication_schema_migrations (
    migration_id TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by TEXT NOT NULL DEFAULT CURRENT_USER
);

CREATE TABLE IF NOT EXISTS public.recommendation_generations (
    generation_id VARCHAR(80) PRIMARY KEY,
    airflow_run_id TEXT,
    status VARCHAR(16) NOT NULL CHECK (
        status IN ('BUILDING', 'READY', 'ACTIVE', 'SUPERSEDED', 'FAILED')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    validated_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    previous_generation_id VARCHAR(80) REFERENCES public.recommendation_generations(generation_id),
    manifest JSONB NOT NULL,
    validation_report JSONB,
    failure_report JSONB
);

ALTER TABLE public.recommendation_generations
    ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS failure_report JSONB;

CREATE UNIQUE INDEX IF NOT EXISTS uq_recommendation_generations_airflow_run
    ON public.recommendation_generations (airflow_run_id)
    WHERE airflow_run_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_recommendation_generations_one_active
    ON public.recommendation_generations (status)
    WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS public.active_recommendation_generation (
    singleton_key SMALLINT PRIMARY KEY CHECK (singleton_key = 1),
    generation_id VARCHAR(80) REFERENCES public.recommendation_generations(generation_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.active_recommendation_generation (singleton_key, generation_id)
VALUES (1, NULL)
ON CONFLICT (singleton_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.recommendation_generation_components (
    generation_id VARCHAR(80) NOT NULL REFERENCES public.recommendation_generations(generation_id),
    component_name VARCHAR(64) NOT NULL CHECK (
        component_name IN (
            'user_clusters',
            'cluster_recommendations',
            'als',
            'content_based',
            'item_based'
        )
    ),
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    completion_status VARCHAR(16) NOT NULL CHECK (completion_status IN ('COMPLETE', 'FAILED')),
    checksum TEXT,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_info JSONB NOT NULL,
    PRIMARY KEY (generation_id, component_name)
);

CREATE TABLE IF NOT EXISTS public.serving_user_clusters_versions (
    generation_id VARCHAR(80) NOT NULL REFERENCES public.recommendation_generations(generation_id),
    user_id TEXT NOT NULL,
    cluster_id INTEGER NOT NULL,
    segment_name TEXT NOT NULL,
    PRIMARY KEY (generation_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_serving_user_clusters_versions_cluster
    ON public.serving_user_clusters_versions (generation_id, cluster_id);

CREATE TABLE IF NOT EXISTS public.serving_recommendations_versions (
    generation_id VARCHAR(80) NOT NULL REFERENCES public.recommendation_generations(generation_id),
    cluster_id INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    cluster_total_score DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (generation_id, cluster_id, product_id)
);

CREATE TABLE IF NOT EXISTS public.serving_als_versions (
    generation_id VARCHAR(80) NOT NULL REFERENCES public.recommendation_generations(generation_id),
    user_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (generation_id, user_id, product_id)
);

CREATE TABLE IF NOT EXISTS public.serving_content_based_versions (
    generation_id VARCHAR(80) NOT NULL REFERENCES public.recommendation_generations(generation_id),
    user_id TEXT,
    product_id TEXT,
    source_product_id TEXT,
    recommended_product_id TEXT,
    score DOUBLE PRECISION NOT NULL,
    CHECK (
        (user_id IS NOT NULL AND product_id IS NOT NULL
            AND source_product_id IS NULL AND recommended_product_id IS NULL)
        OR
        (source_product_id IS NOT NULL AND recommended_product_id IS NOT NULL
            AND user_id IS NULL AND product_id IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_serving_content_versions_user
    ON public.serving_content_based_versions (generation_id, user_id, product_id)
    WHERE user_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_serving_content_versions_item
    ON public.serving_content_based_versions (
        generation_id,
        source_product_id,
        recommended_product_id
    )
    WHERE source_product_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.serving_item_based_versions (
    generation_id VARCHAR(80) NOT NULL REFERENCES public.recommendation_generations(generation_id),
    source_product_id TEXT NOT NULL,
    similar_product_id TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (generation_id, source_product_id, similar_product_id)
);

CREATE TABLE IF NOT EXISTS public.recommendation_generation_rollbacks (
    rollback_id BIGSERIAL PRIMARY KEY,
    from_generation_id VARCHAR(80) REFERENCES public.recommendation_generations(generation_id),
    to_generation_id VARCHAR(80) NOT NULL REFERENCES public.recommendation_generations(generation_id),
    rolled_back_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
);

-- Correctness guard for late or retried Spark JDBC tasks. This row-level
-- lifecycle check has runtime cost and must be measured under real export load.
CREATE OR REPLACE FUNCTION public.guard_building_generation_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $guard$
DECLARE
    old_status VARCHAR(16);
    new_status VARCHAR(16);
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT status INTO new_status
        FROM public.recommendation_generations
        WHERE generation_id = NEW.generation_id
        FOR SHARE;
        IF new_status IS DISTINCT FROM 'BUILDING' THEN
            RAISE EXCEPTION 'Versioned serving INSERT requires a BUILDING generation';
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        SELECT status INTO old_status
        FROM public.recommendation_generations
        WHERE generation_id = OLD.generation_id
        FOR SHARE;
        SELECT status INTO new_status
        FROM public.recommendation_generations
        WHERE generation_id = NEW.generation_id
        FOR SHARE;
        IF old_status IS DISTINCT FROM 'BUILDING'
           OR new_status IS DISTINCT FROM 'BUILDING' THEN
            RAISE EXCEPTION 'Versioned serving UPDATE requires BUILDING old and new generations';
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        SELECT status INTO old_status
        FROM public.recommendation_generations
        WHERE generation_id = OLD.generation_id
        FOR SHARE;
        IF old_status IS DISTINCT FROM 'BUILDING' THEN
            RAISE EXCEPTION 'Versioned serving DELETE requires a BUILDING generation';
        END IF;
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'Unsupported operation for versioned serving guard: %', TG_OP;
END;
$guard$;

DO $attach_guards$
DECLARE
    protected_table TEXT;
BEGIN
    FOREACH protected_table IN ARRAY ARRAY[
        'serving_user_clusters_versions',
        'serving_recommendations_versions',
        'serving_als_versions',
        'serving_content_based_versions',
        'serving_item_based_versions'
    ]
    LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_trigger
            WHERE tgname = 'trg_guard_building_generation'
              AND tgrelid = pg_catalog.to_regclass('public.' || protected_table)
              AND NOT tgisinternal
        ) THEN
            EXECUTE pg_catalog.format(
                'CREATE TRIGGER trg_guard_building_generation '
                'BEFORE INSERT OR UPDATE OR DELETE ON public.%I '
                'FOR EACH ROW EXECUTE FUNCTION public.guard_building_generation_mutation()',
                protected_table
            );
        END IF;
    END LOOP;
END;
$attach_guards$;

-- Future cleanup contract (intentionally not automated here):
-- 1. Retain the active generation and at least its previous generation.
-- 2. Never delete BUILDING generations owned by a running DAG.
-- 3. Delete only generations older than a configured retention window.
-- 4. Run cleanup outside publication and rollback transactions.
