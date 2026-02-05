CREATE TABLE members (
    player_tag TEXT PRIMARY KEY,
    player_name TEXT NOT NULL,
    join_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE member_snapshots (
    snapshot_id SERIAL PRIMARY KEY,
    player_tag TEXT NOT NULL,
    snapshot_date DATE,
    role TEXT,
    trophies INT,
    league_name TEXT,
    league_id INT,
    town_hall_level INT,
    donations_given INT,
    donations_received INT,
    attack_wins INT,
    defense_wins INT,
    war_stars INT,
    clan_rank INT,
    FOREIGN KEY (player_tag) REFERENCES members(player_tag),
    UNIQUE (player_tag, snapshot_date)
);

CREATE TABLE wars (
    war_id SERIAL PRIMARY KEY,
    end_time TIMESTAMP,
    result TEXT NOT NULL,
    team_size INT,
    clan_stars INT,
    opponent_stars INT,
    clan_destruction FLOAT,
    opponent_destruction FLOAT,
    opponent_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE war_participation (
    participation_id SERIAL PRIMARY KEY,
    war_id INT NOT NULL,
    player_tag TEXT NOT NULL,
    attacks_used INT,
    stars_earned INT,
    destruction_percent FLOAT,
    FOREIGN KEY (war_id) REFERENCES wars(war_id),
    FOREIGN KEY (player_tag) REFERENCES members(player_tag),
    UNIQUE(war_id, player_tag)
);
