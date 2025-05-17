CREATE TABLE IF NOT EXISTS protocol_reports (
    id SERIAL PRIMARY KEY,
    week_start DATE,
    financial_year TEXT,
    financial_week INTEGER,
    week_number INTEGER,
    r_date TIMESTAMP,
    day_name TEXT,
    r_time TIME,
    id_tag TEXT UNIQUE,
    r_profile TEXT,
    coffee_inv TEXT,
    pg_code TEXT,
    green_vol FLOAT,
    end_weight FLOAT,
    machine TEXT
);