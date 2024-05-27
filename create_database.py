import sqlite3

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('mlb_stats.db')
cursor = conn.cursor()

# Create the player_stats table with the correct schema
cursor.execute('''
CREATE TABLE IF NOT EXISTS player_stats (
    Split TEXT,
    GP INTEGER,
    GS INTEGER,
    CG INTEGER,
    SHO INTEGER,
    IP REAL,
    H INTEGER,
    R INTEGER,
    ER INTEGER,
    HR INTEGER,
    BB INTEGER,
    K INTEGER,
    PlayerName TEXT,
    TeamName TEXT
)
''')

# Commit the changes and close the connection
conn.commit()
conn.close()
