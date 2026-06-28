BEGIN;

CREATE TABLE IF NOT EXISTS public.recommendation_generations (
    generation_id VARCHAR(80) PRIMARY KEY,
    airflow_run_id TEXT,
    status VARCHAR(16) NOT NULL CHECK (
        status IN ('BUILDING', 'READY', 'ACTIVE', 'SUPERSEDED', 'FAILED')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    validated_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    previous_generation_id VARCHAR(80) REFERENCES public.recommendation_generations(generation_id),
    manifest JSONB NOT NULL,
    validation_report JSONB
);

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

COMMIT;

-- Future cleanup contract (intentionally not automated here):
-- 1. Retain the active generation and at least its previous generation.
-- 2. Never delete BUILDING generations owned by a running DAG.
-- 3. Delete only generations older than a configured retention window.
-- 4. Run cleanup outside publication and rollback transactions.
