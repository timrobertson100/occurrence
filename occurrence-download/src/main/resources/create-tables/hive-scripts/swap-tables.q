USE ${hiveDB};

-- Rename old tables
ALTER TABLE ${occurrenceTable}_avro RENAME TO old_${occurrenceTable}_avro;
ALTER TABLE ${occurrenceTable}_hdfs RENAME TO old_${occurrenceTable}_hdfs;
ALTER TABLE ${occurrenceTable}_multimedia RENAME TO old_${occurrenceTable}_multimedia;


-- Rename new tables
ALTER TABLE new_${occurrenceTable}_avro RENAME TO ${occurrenceTable}_avro;
ALTER TABLE new_${occurrenceTable}_hdfs RENAME TO ${occurrenceTable}_hdfs;
ALTER TABLE new_${occurrenceTable}_multimedia RENAME TO ${occurrenceTable}_multimedia;



