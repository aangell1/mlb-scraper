import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

# Database setup
engine = create_engine('sqlite:///mlb_stats.db')

# Function to fetch HTML content
def fetch_html(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text

# Function to scrape team data
def scrape_team_data(team_url):
    print(f"Fetching team page: {team_url}")
    team_html = fetch_html(team_url)
    soup = BeautifulSoup(team_html, 'html.parser')
    
    try:
        team_name = soup.find('span', class_='db fw-bold').text.strip()
    except AttributeError:
        team_name = "Team name not found"
    
    print(f"Scraped team name: {team_name}")

    player_data = []
    
    # Process players
    roster_table = soup.find_all('table', class_='Table')
    for table in roster_table:
        for row in table.find_all('tr')[1:]:  # Skip the header row
            cols = row.find_all('td')
            if len(cols) > 0:
                player_link = cols[1].find('a', class_='AnchorLink')
                player_position = cols[0].text.strip()  # Assuming the position is in the first column
                if player_link:
                    player_name = player_link.text.strip()
                    player_url = player_link['href']
                    if not player_url.startswith('http'):
                        player_url = 'https://www.espn.com' + player_url
                    print(f"Found player: {player_name}, URL: {player_url}, Position: {player_position}")
                    player_data.append((player_name, player_url, player_position))

    print(f"Found {len(player_data)} players for {team_name}")
    return team_name, player_data

# Function to fetch and store player stats
def fetch_and_store_player_stats(player_data, team_name, engine):
    for player_name, player_url, player_position in player_data:
        print(f"Fetching data for {player_name} from {player_url}")
        player_html = fetch_html(player_url)
        player_soup = BeautifulSoup(player_html, 'html.parser')
        
        stats_table = player_soup.find('table', class_='Table Table--align-right')
        if stats_table:
            stats_df = pd.read_html(str(stats_table))[0]
            stats_df['PlayerName'] = player_name
            stats_df['TeamName'] = team_name
            stats_df['Position'] = player_position
            
            print(f"Stats for {player_name}:")
            print(stats_df.head())
            
            # Adjust column names to match database schema
            if 'Splits' in stats_df.columns:
                stats_df.rename(columns={'Splits': 'Split'}, inplace=True)
            
            # Ensure all columns are present
            columns = [
                'Split', 'GP', 'GS', 'CG', 'SHO', 'IP', 'H', 'R', 'ER', 'HR', 'BB', 'K',
                'AB', '2B', '3B', 'RBI', 'SO', 'PlayerName', 'TeamName', 'Position'
            ]
            for col in columns:
                if col not in stats_df.columns:
                    stats_df[col] = None
            
            # Insert data into the database
            try:
                stats_df.to_sql('player_stats', con=engine, if_exists='append', index=False)
                print(f"Data for {player_name} inserted into the database.")
            except Exception as e:
                print(f"Error inserting data for {player_name}: {e}")
        else:
            print(f"No stats found for {player_name}")

# Main function to iterate over all teams
def main():
    teams = [
        ('Arizona Diamondbacks', 'https://www.espn.com/mlb/team/roster/_/name/ari'),
        ('Atlanta Braves', 'https://www.espn.com/mlb/team/roster/_/name/atl'),
        ('Baltimore Orioles', 'https://www.espn.com/mlb/team/roster/_/name/bal'),
        ('Boston Red Sox', 'https://www.espn.com/mlb/team/roster/_/name/bos'),
        ('Chicago White Sox', 'https://www.espn.com/mlb/team/roster/_/name/chw'),
        ('Chicago Cubs', 'https://www.espn.com/mlb/team/roster/_/name/chc'),
        ('Cincinnati Reds', 'https://www.espn.com/mlb/team/roster/_/name/cin'),
        ('Cleveland Guardians', 'https://www.espn.com/mlb/team/roster/_/name/cle'),
        ('Colorado Rockies', 'https://www.espn.com/mlb/team/roster/_/name/col'),
        ('Detroit Tigers', 'https://www.espn.com/mlb/team/roster/_/name/det'),
        ('Houston Astros', 'https://www.espn.com/mlb/team/roster/_/name/hou'),
        ('Kansas City Royals', 'https://www.espn.com/mlb/team/roster/_/name/kc'),
        ('Los Angeles Angels', 'https://www.espn.com/mlb/team/roster/_/name/laa'),
        ('Los Angeles Dodgers', 'https://www.espn.com/mlb/team/roster/_/name/lad'),
        ('Miami Marlins', 'https://www.espn.com/mlb/team/roster/_/name/mia'),
        ('Milwaukee Brewers', 'https://www.espn.com/mlb/team/roster/_/name/mil'),
        ('Minnesota Twins', 'https://www.espn.com/mlb/team/roster/_/name/min'),
        ('New York Yankees', 'https://www.espn.com/mlb/team/roster/_/name/nyy'),
        ('New York Mets', 'https://www.espn.com/mlb/team/roster/_/name/nym'),
        ('Oakland Athletics', 'https://www.espn.com/mlb/team/roster/_/name/oak'),
        ('Philadelphia Phillies', 'https://www.espn.com/mlb/team/roster/_/name/phi'),
        ('Pittsburgh Pirates', 'https://www.espn.com/mlb/team/roster/_/name/pit'),
        ('San Diego Padres', 'https://www.espn.com/mlb/team/roster/_/name/sd'),
        ('San Francisco Giants', 'https://www.espn.com/mlb/team/roster/_/name/sf'),
        ('Seattle Mariners', 'https://www.espn.com/mlb/team/roster/_/name/sea'),
        ('St. Louis Cardinals', 'https://www.espn.com/mlb/team/roster/_/name/stl'),
        ('Tampa Bay Rays', 'https://www.espn.com/mlb/team/roster/_/name/tb'),
        ('Texas Rangers', 'https://www.espn.com/mlb/team/roster/_/name/tex'),
        ('Toronto Blue Jays', 'https://www.espn.com/mlb/team/roster/_/name/tor'),
        ('Washington Nationals', 'https://www.espn.com/mlb/team/roster/_/name/wsh')
    ]
    
    for team_name, team_url in teams:
        print(f"Scraping data for {team_name}")
        team_name, player_data = scrape_team_data(team_url)
        fetch_and_store_player_stats(player_data, team_name, engine)
    
    print("Scraping completed and data inserted into the database.")

if __name__ == "__main__":
    main()
