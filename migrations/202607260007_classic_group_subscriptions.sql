ALTER TABLE consumer_group_members
    ADD COLUMN IF NOT EXISTS subscribed_topics TEXT[] NOT NULL DEFAULT '{}';
