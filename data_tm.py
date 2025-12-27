import requests
from bs4 import BeautifulSoup
import time
import os
import random
import re

# --- SCRIPT SETTINGS ---
TEST_ONE_TEAM_ONLY = False  

# --- Dirs ---
NODE_DIR = "tm_nodes/"
REL_DIR = "tm_relationships/"
os.makedirs(NODE_DIR, exist_ok=True)
os.makedirs(REL_DIR, exist_ok=True)

PROCESSED_TEAMS_FILE = "tm_processed_teams.txt"
PROCESSED_PLAYERS_FILE = "tm_processed_players.txt"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

BASE_URL = "https://www.transfermarkt.com"

LEAGUES = {
    "Botola Pro": {"url": "/botola-pro-inwi/startseite/wettbewerb/MAR1", "country": "Morocco"},
    "Premier League": {"url": "/premier-league/startseite/wettbewerb/GB1", "country": "England"},
    "La Liga": {"url": "/laliga/startseite/wettbewerb/ES1", "country": "Spain"},
    "Serie A": {"url": "/serie-a/startseite/wettbewerb/IT1", "country": "Italy"},
    "Bundesliga": {"url": "/bundesliga/startseite/wettbewerb/L1", "country": "Germany"},
    "Ligue 1": {"url": "/ligue-1/startseite/wettbewerb/FR1", "country": "France"}
}

# --- Helpers ---
def get_soup(url_path):
    full_url = f"{BASE_URL}{url_path}" if not url_path.startswith("http") else url_path
    time.sleep(random.uniform(0.6, 1.2)) 
    try:
        response = requests.get(full_url, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            return BeautifulSoup(response.content, "html.parser")
        elif response.status_code == 429:
            print("  ⚠️ Rate limit! Sleeping 60s...")
            time.sleep(60)
            return get_soup(url_path)
    except: return None
    return None

def clean_str(val):
    if val is None: return ""
    val = re.sub(r'^#\d+\s*', '', str(val))
    return f'"{str(val).strip().replace('"', '""')}"'

def clean_val(val):
    if val is None: return "0"
    val = str(val).strip()
    val = val.replace("'", "").replace(".", "") # Remove minutes ' and dots
    if val in ["-", "", "None"]: return "0"
    if "/" in val: return val # Return slash strings as-is
    if not val.replace('-', '').isdigit(): return "0" 
    return val

def load_ids(f):
    if not os.path.exists(f): return set()
    with open(f, 'r') as file: return {line.strip() for line in file if line.strip()}

def save_id(f, i):
    with open(f, 'a') as file: file.write(f"{i}\n")

def save_csv(folder, filename, row):
    with open(os.path.join(folder, filename), 'a', encoding='utf-8') as f: f.write(row + "\n")

# --- Setup ---
def setup_csv_files():
    print("Initializing CSV files...")
    
    node_headers = {
        "players.csv": "id,name,age,nationality,jerseyNumber,height,weight,preferred_foot,preferred_positions,market_value,overall_rating,minutes_played,current_club_id",
        "teams.csv": "id,name,league_name",
        "managers.csv": "id,name,age,nationality", 
        "leagues.csv": "id,name,tier,fifa_ranking",
        "countries.csv": "name",
        "contracts.csv": "id,joined_date,expires_date,market_value,salary",
        "stats.csv": "id,total_matches,total_goals,total_assists,total_yellow,total_second_yellow,total_red,goals_conceded,clean_sheets,win_rate,possession,passing_accuracy,shots_per_game,rating",
        "achievements.csv": "id,title,year,competition,level",
        "injuries.csv": "id,type,start_date,end_date,severity,description"
    }
    
    rel_headers = {
        "player_plays_for.csv": "player_id,team_id",
        "team_participates_in.csv": "team_id,league_id",
        "team_based_in.csv": "team_id,country_name",
        "team_managed_by.csv": "team_id,manager_id",
        "manager_belongs_to.csv": "manager_id,country_name",
        "manager_manages.csv": "manager_id,team_id",
        "manager_has_achievement.csv": "manager_id,ach_id",
        "player_plays_for_country.csv": "player_id,country_name",
        "player_has_stats.csv": "player_id,stat_id",
        "stats_for_player.csv": "stat_id,player_id",
        "player_has_achievement.csv": "player_id,ach_id",
        "player_has_injury.csv": "player_id,inj_id",
        "injury_affected.csv": "inj_id,player_id",
        "player_has_contract.csv": "player_id,cont_id",
        "contract_associated_with.csv": "cont_id,associated_id,type",
        "contract_from_team.csv": "cont_id,team_id",
        "contract_to_team.csv": "cont_id,team_id",
        "league_located_in.csv": "league_id,country_name",
        "achievement_won_by.csv": "ach_id,winner_id,type",
        "manager_career_history.csv": "manager_id,team_id,team_name,start_date,end_date"
    }

    for d, headers in [(NODE_DIR, node_headers), (REL_DIR, rel_headers)]:
        for fname, header in headers.items():
            path = os.path.join(d, fname)
            if not os.path.exists(path):
                with open(path, 'w', encoding='utf-8') as f: f.write(header + "\n")

# --- Parsing Logic ---

def get_profile_value(soup, label_list):
    if isinstance(label_list, str): label_list = [label_list]
    for label_text in label_list:
        label = soup.find(string=re.compile(label_text))
        if not label: continue
        container = label.find_parent('li') or label.find_parent('tr')
        if container:
            val = container.find(class_='data-header__content') or \
                  container.find(class_='info-table__content--bold')
            if val: return val.get_text(strip=True)
        parent_span = label.find_parent('span')
        if parent_span:
             val = parent_span.find_next_sibling('span', class_='info-table__content--bold')
             if val: return val.get_text(strip=True)
    return ""

def scrape_manager_details(manager_url):
    soup = get_soup(manager_url)
    if not soup: return None, None
    age = ""
    dob_label = soup.find(string=re.compile("Date of birth/Age"))
    if dob_label:
        container = dob_label.find_parent('li') or dob_label.find_parent('tr')
        if container:
            txt = container.get_text(strip=True)
            age_match = re.search(r'\((\d+)\)', txt)
            if age_match: age = age_match.group(1)
    nat = ""
    nat_label = soup.find(string=re.compile("Citizenship"))
    if nat_label:
        nat_container = nat_label.find_parent('li') or nat_label.find_parent('tr')
        if nat_container:
            flag = nat_container.find('img', class_='flaggenrahmen')
            nat = flag.get('title', '') if flag else ""
    return age, nat

def scrape_player(player_url, team_id, processed_players):
    p_id = player_url.split('/')[-1]
    if p_id in processed_players: return

    soup = get_soup(player_url)
    if not soup: return

    # 1. PROFILE
    try:
        h1 = soup.find('h1')
        if h1:
            for tag in h1.select('span'): tag.decompose() 
            name = h1.get_text(" ", strip=True) 
        else: name = "Unknown"

        print(f"    Processing: {name} (ID: {p_id})")

        age = ""
        dob_label = soup.find(string=re.compile("Date of birth/Age"))
        if dob_label:
            container = dob_label.find_parent('li') or dob_label.find_parent('tr')
            if container:
                full_text = container.get_text(strip=True)
                match = re.search(r'\((\d+)\)', full_text)
                if match: age = match.group(1)

        height = get_profile_value(soup, "Height").replace('m', '').replace(',', '.')
        foot = get_profile_value(soup, ["Foot", "Main foot", "Strong foot"])
        if foot: print(f"      [FOOT FOUND] {foot}")
        pos = get_profile_value(soup, "Position")
        
        nat = ""
        nat_label = soup.find(string=re.compile("Citizenship"))
        if nat_label:
            nat_container = nat_label.find_parent('li') or nat_label.find_parent('tr')
            if nat_container:
                flag = nat_container.find('img', class_='flaggenrahmen')
                nat = flag.get('title', '') if flag else ""

        mv_box = soup.find('a', class_='data-header__market-value-wrapper')
        mv = mv_box.get_text(strip=True).split('Last')[0] if mv_box else ""

        save_csv(NODE_DIR, "players.csv", f'{p_id},{clean_str(name)},{clean_val(age)},{clean_str(nat)},None,{clean_val(height)},None,{clean_str(foot)},{clean_str(pos)},{clean_str(mv)},None,None,{team_id}')
        save_csv(REL_DIR, "player_plays_for.csv", f"{p_id},{team_id}")
        if nat:
            save_csv(NODE_DIR, "countries.csv", clean_str(nat))
            save_csv(REL_DIR, "player_plays_for_country.csv", f"{p_id},{clean_str(nat)}")
    except Exception as e: print(f"      ❌ Error parsing profile: {e}")

    # 2. STATS - Using column positions (different for GK vs outfield players)
    # Outfield: Col2=Matches, Col3=Goals, Col4=Assists, Col8=Yellow, Col9=SecondYellow, Col10=Red
    # GK: Col2=Matches, Col3=-, Col4=Cards(slash-separated), Col5=GoalsConceded, Col6=CleanSheets
    stats_url = player_url.replace("/profil/", "/leistungsdaten/") + "/plus/1?saison=ges"
    s_soup = get_soup(stats_url)
    if s_soup:
        footer = s_soup.find('tfoot')
        if footer:
            row = footer.find('tr')
            if row:
                cols = row.find_all('td')

                # Check if player is a Goalkeeper by looking at position
                is_goalkeeper = pos and ("Goalkeeper" in pos or "keeper" in pos.lower())

                matches = clean_val(cols[2].get_text(strip=True)) if len(cols) > 2 else "0"

                if is_goalkeeper:
                    # GK table: Col2=Matches, Col7=Yellow, Col8=2ndYellow, Col9=Red, Col10=GoalsConceded, Col11=CleanSheets
                    goals = "0"
                    assists = "0"
                    yellow = clean_val(cols[7].get_text(strip=True)) if len(cols) > 7 else "0"
                    second_yellow = clean_val(cols[8].get_text(strip=True)) if len(cols) > 8 else "0"
                    red = clean_val(cols[9].get_text(strip=True)) if len(cols) > 9 else "0"
                    goals_conceded = clean_val(cols[10].get_text(strip=True)) if len(cols) > 10 else "0"
                    clean_sheets = clean_val(cols[11].get_text(strip=True)) if len(cols) > 11 else "0"

                    print(f"      [GK STATS SAVED] Matches: {matches}, Yellow: {yellow}, 2ndYellow: {second_yellow}, Red: {red}, GoalsConceded: {goals_conceded}, CleanSheets: {clean_sheets}")
                else:
                    # Outfield player table: Matches | Goals | Assists | ... | Yellow | 2ndYellow | Red
                    goals = clean_val(cols[3].get_text(strip=True)) if len(cols) > 3 else "0"
                    assists = clean_val(cols[4].get_text(strip=True)) if len(cols) > 4 else "0"
                    yellow = clean_val(cols[8].get_text(strip=True)) if len(cols) > 8 else "0"
                    second_yellow = clean_val(cols[9].get_text(strip=True)) if len(cols) > 9 else "0"
                    red = clean_val(cols[10].get_text(strip=True)) if len(cols) > 10 else "0"
                    goals_conceded = "0"
                    clean_sheets = "0"

                    print(f"      [TOTAL STATS SAVED] Matches: {matches}, Goals: {goals}, Assists: {assists}, Yellow: {yellow}, 2ndYellow: {second_yellow}, Red: {red}")

                stat_id = f"{p_id}_Total"
                save_csv(NODE_DIR, "stats.csv", f'{clean_str(stat_id)},{matches},{goals},{assists},{yellow},{second_yellow},{red},{goals_conceded},{clean_sheets},None,None,None,None,None')
                save_csv(REL_DIR, "player_has_stats.csv", f"{p_id},{clean_str(stat_id)}")
                save_csv(REL_DIR, "stats_for_player.csv", f"{clean_str(stat_id)},{p_id}")

    # 3. CURRENT CONTRACT
    try:
        joined = get_profile_value(soup, ["Joined", "In squad since"])
        expires = get_profile_value(soup, ["Contract expires", "Contract until"])
        date_pattern = r'(\d{2}[/.]\d{2}[/.]\d{4})|(\w{3} \d{1,2}, \d{4})'
        
        sidebar = soup.find('div', class_='info-table')
        if sidebar:
            full_text = sidebar.get_text(" ", strip=True)
            if not expires:
                match = re.search(r'Contract expires[:\s]+.*?(' + date_pattern + r')', full_text)
                if match: expires = match.group(1) or match.group(2)
            if not joined:
                match = re.search(r'Joined[:\s]+.*?(' + date_pattern + r')', full_text)
                if match: joined = match.group(1) or match.group(2)

        if joined or expires:
            c_id = f"{p_id}_Current"
            print(f"      [CONTRACT] Joined: {joined} | Expires: {expires}")
            save_csv(NODE_DIR, "contracts.csv", f'{clean_str(c_id)},{clean_str(joined)},{clean_str(expires)},{clean_str(mv)},None')
            save_csv(REL_DIR, "player_has_contract.csv", f"{p_id},{clean_str(c_id)}")
            save_csv(REL_DIR, "contract_associated_with.csv", f"{clean_str(c_id)},{p_id},Player")
            save_csv(REL_DIR, "contract_from_team.csv", f"{clean_str(c_id)},{team_id}")
            
    except Exception as e:
        print(f"      [DEBUG] Contract error: {e}")

    # 4. ACHIEVEMENTS
    ach_url = player_url.replace("/profil/", "/erfolge/")
    a_soup = get_soup(ach_url)
    if a_soup:
        boxes = a_soup.find_all('div', class_='box')
        for box in boxes:
            header = box.find('h2')
            if header:
                title_name = header.get_text(strip=True).replace("Titles", "").strip()
                if "Relegat" in title_name or "relegat" in title_name or "All titles" in title_name: continue
                for row in box.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) >= 1:
                        raw_text = row.get_text(" ", strip=True)
                        year_match = re.search(r'\d{2}/\d{2}|\d{4}', raw_text)
                        year = year_match.group(0) if year_match else ""
                        if not year and len(cols) > 2:
                            if re.match(r'\d{2}/\d{2}|\d{4}', cols[2].get_text(strip=True)): 
                                year = cols[2].get_text(strip=True)
                        if year:
                            a_id = f"{p_id}_{title_name}_{year}".replace(" ", "")
                            print(f"      [ACHIEVEMENT FOUND] {title_name} ({year})")
                            save_csv(NODE_DIR, "achievements.csv", f'{clean_str(a_id)},{clean_str(title_name)},{clean_str(year)},{clean_str(title_name)},None')
                            save_csv(REL_DIR, "player_has_achievement.csv", f"{p_id},{clean_str(a_id)}")
                            save_csv(REL_DIR, "achievement_won_by.csv", f"{clean_str(a_id)},{p_id},Player")

    # 5. INJURIES
    inj_url = player_url.replace("/profil/", "/verletzungen/")
    i_soup = get_soup(inj_url)
    if i_soup:
        table = i_soup.find('table', class_='items')
        if table:
            for row in table.select("tbody tr"):
                cols = row.find_all('td')
                if len(cols) >= 6:
                    inj_type = cols[1].get_text(strip=True)
                    start = cols[2].get_text(strip=True)
                    end = cols[3].get_text(strip=True)
                    i_id = f"{p_id}_{start}"
                    save_csv(NODE_DIR, "injuries.csv", f'{clean_str(i_id)},{clean_str(inj_type)},{clean_str(start)},{clean_str(end)},None,None')
                    save_csv(REL_DIR, "player_has_injury.csv", f"{p_id},{clean_str(i_id)}")
                    save_csv(REL_DIR, "injury_affected.csv", f"{clean_str(i_id)},{p_id}")

    save_id(PROCESSED_PLAYERS_FILE, p_id)

def process_league(name, data, processed_teams, processed_players):
    url = data['url']
    country = data['country']
    l_id = url.split('/')[-1]
    
    print(f"\nProcessing League: {name} (Country: {country})")
    save_csv(NODE_DIR, "leagues.csv", f'{clean_str(l_id)},{clean_str(name)},None,None')
    save_csv(REL_DIR, "league_located_in.csv", f"{clean_str(l_id)},{clean_str(country)}")
    save_csv(NODE_DIR, "countries.csv", clean_str(country))

    soup = get_soup(url)
    if not soup: return

    table = soup.find('table', class_='items')
    if not table: return

    teams = []
    for link in table.find_all('a', href=True):
        if "/startseite/verein/" in link['href']:
            t_name = link.get('title')
            t_id = link['href'].split('/')[-3]
            if t_id.isdigit() and t_name and not any(t[0] == t_id for t in teams):
                teams.append((t_id, t_name, link['href']))

    if TEST_ONE_TEAM_ONLY: teams = teams[:1]

    for t_id, t_name, t_url in teams:
        if t_id in processed_teams:
            print(f"  Skipping {t_name}")
            continue

        print(f"  Processing Team: {t_name}")
        save_csv(NODE_DIR, "teams.csv", f'{t_id},{clean_str(t_name)},{clean_str(name)}')
        save_csv(REL_DIR, "team_participates_in.csv", f"{t_id},{l_id}")
        save_csv(REL_DIR, "team_based_in.csv", f"{t_id},{clean_str(country)}")

        t_soup = get_soup(t_url)
        if t_soup:
            m_name, m_id, m_url = None, None, None
            for label in ["Manager:", "Trainer:", "Head Coach:", "Coach:"]:
                label_tag = t_soup.find(string=re.compile(label))
                if label_tag:
                    container = label_tag.find_parent('li') or label_tag.find_parent('tr')
                    if container:
                        m_link = container.find('a', href=re.compile(r"/profil/trainer/"))
                        if m_link:
                            m_name = m_link.get_text(strip=True)
                            m_id = m_link['href'].split('/')[-1]
                            m_url = m_link['href']
                            break
            
            if not m_name:
                print("    ...Manager not on main page, checking Staff page...")
                staff_url = t_url.replace("/startseite/", "/mitarbeiter/")
                staff_soup = get_soup(staff_url)
                if staff_soup:
                    for row in staff_soup.find_all('tr'):
                        role_text = row.get_text(strip=True)
                        if "Manager" in role_text or "Head Coach" in role_text or "Trainer" in role_text:
                            if "Assistant" in role_text or "Goalkeeper" in role_text or "Athletic" in role_text: continue
                            m_link = row.find('a', href=re.compile(r"/profil/trainer/"))
                            if m_link:
                                m_name = m_link.get_text(strip=True)
                                m_id = m_link['href'].split('/')[-1]
                                m_url = m_link['href']
                                break

            if m_name and m_id:
                if not m_name: m_name = "Unknown Manager"
                print(f"    [MANAGER FOUND] {m_name}. Fetching details...")
                m_age, m_nat = scrape_manager_details(m_url)
                save_csv(NODE_DIR, "managers.csv", f'{m_id},{clean_str(m_name)},{clean_val(m_age)},{clean_str(m_nat)}')
                save_csv(REL_DIR, "team_managed_by.csv", f"{t_id},{m_id}")
                save_csv(REL_DIR, "manager_manages.csv", f"{m_id},{t_id}")
                if m_nat:
                    save_csv(NODE_DIR, "countries.csv", clean_str(m_nat))
                    save_csv(REL_DIR, "manager_belongs_to.csv", f"{m_id},{clean_str(m_nat)}")
            else:
                print("    [MANAGER NOT FOUND] - Check manually.")

            if squad_table := t_soup.find('table', class_='items'):
                p_links = []
                for a in squad_table.find_all('a', href=True):
                    if "/profil/spieler/" in a['href']:
                        if a['href'] not in p_links: p_links.append(a['href'])
                p_links = list(set(p_links))
                for p_url in p_links:
                    scrape_player(p_url, t_id, processed_players)

        save_id(PROCESSED_TEAMS_FILE, t_id)

if __name__ == "__main__":
    setup_csv_files()
    pt = load_ids(PROCESSED_TEAMS_FILE)
    pp = load_ids(PROCESSED_PLAYERS_FILE)
    for k, v in LEAGUES.items(): process_league(k, v, pt, pp)
    print("\nDONE.")