CREATE TABLE IF NOT EXISTS "indicator_history" (
  date DATE PRIMARY KEY,
  values json NOT NULL
  );
