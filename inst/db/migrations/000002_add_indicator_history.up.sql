CREATE TABLE IF NOT EXISTS "indicator_history" (
  date DATE PRIMARY KEY,
  values json NOT NULL
  );

GRANT SELECT ON x28.indicator_history TO x28_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON x28.indicator_history TO x28_reader;
