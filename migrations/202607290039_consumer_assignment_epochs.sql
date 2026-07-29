CREATE TABLE IF NOT EXISTS consumer_protocol_assignment_epochs (
    group_id TEXT NOT NULL,
    member_id TEXT NOT NULL,
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    partition_index INTEGER NOT NULL CHECK (partition_index >= 0),
    assignment_epoch INTEGER NOT NULL CHECK (assignment_epoch >= 0),
    PRIMARY KEY (group_id, member_id, topic_id, partition_index),
    FOREIGN KEY (group_id, member_id)
        REFERENCES consumer_protocol_members(group_id, member_id)
        ON DELETE CASCADE
);

INSERT INTO consumer_protocol_assignment_epochs
    (group_id, member_id, topic_id, partition_index, assignment_epoch)
SELECT a.group_id, a.member_id, a.topic_id, partition_index,
       GREATEST(m.member_epoch, 0)
FROM consumer_protocol_assignments a
JOIN consumer_protocol_members m
  ON m.group_id = a.group_id AND m.member_id = a.member_id
CROSS JOIN LATERAL unnest(a.partitions) AS partition_index
WHERE a.assignment_kind IN ('current', 'owned')
ON CONFLICT DO NOTHING;
