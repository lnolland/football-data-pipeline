CREATE TABLE IF NOT EXISTS teams (
  id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  short_name TEXT,
  tla TEXT,
  crest TEXT
);

CREATE TABLE IF NOT EXISTS matches (
  id BIGINT PRIMARY KEY,
  utc_date TIMESTAMPTZ,
  status TEXT,
  matchday INT,
  stage TEXT,
  home_team_id BIGINT REFERENCES teams(id),
  away_team_id BIGINT REFERENCES teams(id),
  home_score INT,
  away_score INT,
  last_updated TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(utc_date);
