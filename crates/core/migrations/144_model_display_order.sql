CREATE TABLE IF NOT EXISTS model_display_order (
    model_id TEXT PRIMARY KEY REFERENCES models(id) ON DELETE CASCADE,
    position INTEGER NOT NULL
);
