import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

def scrape_mauritanian_league():
    # Note: We'll use an example link. If the website changes, we'll just modify the link.
    # This link is an example of a simple sports website (for educational purposes)
    url = "https://www.soccerway.com/teams/mauritania/super-d1/2024/results/" 
    
    print(f"--- Started scraping from: {url} ---")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Ensure the website responded successfully
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Search for tables on the page
        tables = soup.find_all('table')
        
        if not tables:
            print("❌ No tables found on the page. The site may be protected or requires a different approach.")
            return

        all_matches = []

        # We will iterate through the tables (usually the first or second table contains the results)
        for table in tables:
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip the header row
                cols = row.find_all('td')
                if len(cols) >= 5: # Ensure the row has enough data
                    # Extract data (this order depends on the website structure)
                    # We will try to extract: date, home team, goals, away team, goals
                    try:
                        date_text = cols[0].text.strip()
                        home_team = cols[1].text.strip()
                        home_goals = cols[2].text.strip()
                        away_team = cols[3].text.strip()
                        away_goals = cols[4].text.strip()

                        all_matches.append({
                            'date': date_text,
                            'home_team': home_team,
                            'home_goals': home_goals,
                            'away_team': away_team,
                            'away_goals': away_goals,
                            'season': '2024-2025' # We'll fix the current season
                        })
                    except Exception:
                        continue

        if not all_matches:
            print("❌ Tables were found but we could not extract any data from them.")
            return

        # Convert results to DataFrame
        df_scraped = pd.DataFrame(all_matches)
        
        # Simple cleaning of extracted data
        df_scraped['home_goals'] = pd.to_numeric(df_scraped['home_goals'], errors='coerce')
        df_scraped['away_goals'] = pd.to_numeric(df_scraped['away_goals'], errors='coerce')
        df_scraped.dropna(subset=['home_goals', 'away_goals'], inplace=True)

        # 4. Save data to the designated folder
        output_dir = "../data/"
        output_file = os.path.join(output_dir, "current_season_scraped.csv")
        
        # Ensure the directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        df_scraped.to_csv(output_file, index=False)
        print(f"✅ Success! Scraped {len(df_scraped)} matches.")
        print(f"📂 File saved at: {output_file}")

    except Exception as e:
        print(f"❌ An error occurred during scraping: {e}")

if __name__ == "__main__":
    scrape_mauritanian_league()