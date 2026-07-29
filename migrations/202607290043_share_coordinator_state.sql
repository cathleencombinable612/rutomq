ALTER TABLE share_partition_states
    DROP CONSTRAINT IF EXISTS share_partition_states_group_id_fkey;

ALTER TABLE share_partition_states
    ADD COLUMN leader_epoch INTEGER NOT NULL DEFAULT -1,
    ADD COLUMN delivery_complete_count INTEGER NOT NULL DEFAULT -1,
    ADD COLUMN state_batches JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE share_partition_states
    ADD CONSTRAINT share_partition_states_leader_epoch_check
        CHECK (leader_epoch >= -1),
    ADD CONSTRAINT share_partition_states_delivery_complete_count_check
        CHECK (delivery_complete_count >= -1),
    ADD CONSTRAINT share_partition_states_state_batches_check
        CHECK (jsonb_typeof(state_batches) = 'array');

ALTER TABLE share_record_states
    ADD COLUMN delivery_state SMALLINT NOT NULL DEFAULT 0;

ALTER TABLE share_record_states
    ADD CONSTRAINT share_record_states_delivery_state_check
        CHECK (delivery_state IN (0, 2, 4));
