"""
Script to load football knowledge graph data into Neo4j.

Requirements:
    pip install neo4j pandas python-dotenv

Usage:
    1. Start Neo4j (Desktop or Docker)
    2. Copy .env.example to .env and update credentials
    3. Run: python load_to_neo4j.py
"""

import os
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Neo4j connection settings from environment
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
NODES_DIR = os.path.join(BASE_DIR, "tm_nodes")
RELATIONSHIPS_DIR = os.path.join(BASE_DIR, "tm_relationships")


class Neo4jLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return result.consume()

    def run_query_batch(self, query, data, batch_size=1000):
        """Execute query in batches for better performance."""
        with self.driver.session() as session:
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                session.run(query, {"batch": batch})

    def clear_database(self):
        """Clear all nodes and relationships."""
        print("Clearing existing data...")
        self.run_query("MATCH (n) DETACH DELETE n")

    def create_constraints(self):
        """Create uniqueness constraints for better performance."""
        print("Creating constraints...")
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Player) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Team) REQUIRE t.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:League) REQUIRE l.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Country) REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Manager) REQUIRE m.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Achievement) REQUIRE a.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Contract) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Injury) REQUIRE i.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Stats) REQUIRE s.id IS UNIQUE",
        ]
        for constraint in constraints:
            try:
                self.run_query(constraint)
            except Exception as e:
                print(f"  Constraint may already exist: {e}")

    # ===================== NODE LOADERS =====================

    def load_players(self):
        """Load player nodes."""
        print("Loading players...")
        df = pd.read_csv(os.path.join(NODES_DIR, "players.csv"))

        # Handle numeric columns - convert to proper types, use None for missing
        numeric_cols = ['age', 'height', 'current_club_id']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Fill string columns with empty string, keep numeric NaN as None
        string_cols = ['name', 'nationality', 'preferred_foot', 'preferred_positions', 'market_value']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna("")

        query = """
        UNWIND $batch AS row
        MERGE (p:Player {id: row.id})
        SET p.name = row.name,
            p.age = CASE WHEN row.age IS NULL THEN null ELSE toInteger(row.age) END,
            p.nationality = row.nationality,
            p.height = CASE WHEN row.height IS NULL THEN null ELSE toInteger(row.height) END,
            p.preferred_foot = row.preferred_foot,
            p.preferred_positions = row.preferred_positions,
            p.market_value = row.market_value,
            p.current_club_id = row.current_club_id
        """
        # Convert NaN to None for JSON serialization
        data = df.where(pd.notnull(df), None).to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} players")

    def load_teams(self):
        """Load team nodes."""
        print("Loading teams...")
        df = pd.read_csv(os.path.join(NODES_DIR, "teams.csv"))
        df = df.dropna(subset=['id'])
        df = df.fillna("")

        query = """
        UNWIND $batch AS row
        MERGE (t:Team {id: row.id})
        SET t.name = row.name,
            t.league_name = row.league_name
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} teams")

    def load_leagues(self):
        """Load league nodes."""
        print("Loading leagues...")
        df = pd.read_csv(os.path.join(NODES_DIR, "leagues.csv"))
        df = df.dropna(subset=['id'])
        df = df.fillna("")

        query = """
        UNWIND $batch AS row
        MERGE (l:League {id: row.id})
        SET l.name = row.name
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} leagues")

    def load_countries(self):
        """Load country nodes."""
        print("Loading countries...")
        df = pd.read_csv(os.path.join(NODES_DIR, "countries.csv"))
        df = df.dropna(subset=['name'])
        df = df.drop_duplicates(subset=['name'])

        query = """
        UNWIND $batch AS row
        MERGE (c:Country {name: row.name})
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} countries")

    def load_managers(self):
        """Load manager nodes."""
        print("Loading managers...")
        df = pd.read_csv(os.path.join(NODES_DIR, "managers.csv"))
        df = df.dropna(subset=['id'])
        df = df.fillna("")

        query = """
        UNWIND $batch AS row
        MERGE (m:Manager {id: row.id})
        SET m.name = row.name,
            m.age = row.age,
            m.nationality = row.nationality
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} managers")

    def load_achievements(self):
        """Load achievement nodes."""
        print("Loading achievements...")
        df = pd.read_csv(os.path.join(NODES_DIR, "achievements.csv"))
        df = df.dropna(subset=['id'])
        df = df.fillna("")

        query = """
        UNWIND $batch AS row
        MERGE (a:Achievement {id: row.id})
        SET a.title = row.title,
            a.year = row.year,
            a.competition = row.competition
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} achievements")

    def load_contracts(self):
        """Load contract nodes."""
        print("Loading contracts...")
        df = pd.read_csv(os.path.join(NODES_DIR, "contracts.csv"))
        df = df.dropna(subset=['id'])
        df = df.fillna("")

        query = """
        UNWIND $batch AS row
        MERGE (c:Contract {id: row.id})
        SET c.joined_date = row.joined_date,
            c.expires_date = row.expires_date,
            c.market_value = row.market_value
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} contracts")

    def load_injuries(self):
        """Load injury nodes."""
        print("Loading injuries...")
        df = pd.read_csv(os.path.join(NODES_DIR, "injuries.csv"))
        df = df.dropna(subset=['id'])
        df = df.fillna("")

        query = """
        UNWIND $batch AS row
        MERGE (i:Injury {id: row.id})
        SET i.type = row.type,
            i.start_date = row.start_date,
            i.end_date = row.end_date
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} injuries")

    def load_stats(self):
        """Load stats nodes."""
        print("Loading stats...")
        df = pd.read_csv(os.path.join(NODES_DIR, "stats.csv"))
        df = df.dropna(subset=['id'])

        # Convert all numeric columns to proper integers
        numeric_cols = ['total_matches', 'total_goals', 'total_assists', 'total_yellow',
                       'total_second_yellow', 'total_red', 'goals_conceded', 'clean_sheets']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        query = """
        UNWIND $batch AS row
        MERGE (s:Stats {id: row.id})
        SET s.total_matches = toInteger(row.total_matches),
            s.total_goals = toInteger(row.total_goals),
            s.total_assists = toInteger(row.total_assists),
            s.total_yellow = toInteger(row.total_yellow),
            s.total_second_yellow = toInteger(row.total_second_yellow),
            s.total_red = toInteger(row.total_red),
            s.goals_conceded = toInteger(row.goals_conceded),
            s.clean_sheets = toInteger(row.clean_sheets)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} stats")

    # ===================== RELATIONSHIP LOADERS =====================

    def load_player_plays_for(self):
        """Load PLAYS_FOR relationships between players and teams."""
        print("Loading PLAYS_FOR relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "player_plays_for.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (p:Player {id: row.player_id})
        MATCH (t:Team {id: row.team_id})
        MERGE (p)-[:PLAYS_FOR]->(t)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_player_plays_for_country(self):
        """Load PLAYS_FOR_COUNTRY relationships."""
        print("Loading PLAYS_FOR_COUNTRY relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "player_plays_for_country.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (p:Player {id: row.player_id})
        MATCH (c:Country {name: row.country_name})
        MERGE (p)-[:PLAYS_FOR_COUNTRY]->(c)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_team_participates_in(self):
        """Load PARTICIPATES_IN relationships between teams and leagues."""
        print("Loading PARTICIPATES_IN relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "team_participates_in.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (t:Team {id: row.team_id})
        MATCH (l:League {id: row.league_id})
        MERGE (t)-[:PARTICIPATES_IN]->(l)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_team_based_in(self):
        """Load BASED_IN relationships between teams and countries."""
        print("Loading BASED_IN relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "team_based_in.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (t:Team {id: row.team_id})
        MATCH (c:Country {name: row.country_name})
        MERGE (t)-[:BASED_IN]->(c)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_manager_manages(self):
        """Load MANAGES relationships between managers and teams."""
        print("Loading MANAGES relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "manager_manages.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (m:Manager {id: row.manager_id})
        MATCH (t:Team {id: row.team_id})
        MERGE (m)-[:MANAGES]->(t)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_manager_belongs_to(self):
        """Load BELONGS_TO relationships between managers and countries."""
        print("Loading manager BELONGS_TO relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "manager_belongs_to.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (m:Manager {id: row.manager_id})
        MATCH (c:Country {name: row.country_name})
        MERGE (m)-[:BELONGS_TO]->(c)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_league_located_in(self):
        """Load LOCATED_IN relationships between leagues and countries."""
        print("Loading LOCATED_IN relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "league_located_in.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (l:League {id: row.league_id})
        MATCH (c:Country {name: row.country_name})
        MERGE (l)-[:LOCATED_IN]->(c)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_player_has_achievement(self):
        """Load HAS_ACHIEVEMENT relationships."""
        print("Loading HAS_ACHIEVEMENT relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "player_has_achievement.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (p:Player {id: row.player_id})
        MATCH (a:Achievement {id: row.ach_id})
        MERGE (p)-[:HAS_ACHIEVEMENT]->(a)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_player_has_contract(self):
        """Load HAS_CONTRACT relationships."""
        print("Loading HAS_CONTRACT relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "player_has_contract.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (p:Player {id: row.player_id})
        MATCH (c:Contract {id: row.cont_id})
        MERGE (p)-[:HAS_CONTRACT]->(c)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_contract_from_team(self):
        """Load FROM_TEAM relationships between contracts and teams."""
        print("Loading contract FROM_TEAM relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "contract_from_team.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (c:Contract {id: row.cont_id})
        MATCH (t:Team {id: row.team_id})
        MERGE (c)-[:FROM_TEAM]->(t)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_player_has_injury(self):
        """Load HAS_INJURY relationships."""
        print("Loading HAS_INJURY relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "player_has_injury.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (p:Player {id: row.player_id})
        MATCH (i:Injury {id: row.inj_id})
        MERGE (p)-[:HAS_INJURY]->(i)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_player_has_stats(self):
        """Load HAS_STATS relationships."""
        print("Loading HAS_STATS relationships...")
        df = pd.read_csv(os.path.join(RELATIONSHIPS_DIR, "player_has_stats.csv"))
        df = df.dropna()

        query = """
        UNWIND $batch AS row
        MATCH (p:Player {id: row.player_id})
        MATCH (s:Stats {id: row.stat_id})
        MERGE (p)-[:HAS_STATS]->(s)
        """
        data = df.to_dict('records')
        self.run_query_batch(query, data)
        print(f"  Loaded {len(data)} relationships")

    def load_all(self):
        """Load all nodes and relationships."""
        # Clear and setup
        self.clear_database()
        self.create_constraints()

        # Load nodes
        print("\n=== Loading Nodes ===")
        self.load_players()
        self.load_teams()
        self.load_leagues()
        self.load_countries()
        self.load_managers()
        self.load_achievements()
        self.load_contracts()
        self.load_injuries()
        self.load_stats()

        # Load relationships
        print("\n=== Loading Relationships ===")
        self.load_player_plays_for()
        self.load_player_plays_for_country()
        self.load_team_participates_in()
        self.load_team_based_in()
        self.load_manager_manages()
        self.load_manager_belongs_to()
        self.load_league_located_in()
        self.load_player_has_achievement()
        self.load_player_has_contract()
        self.load_contract_from_team()
        self.load_player_has_injury()
        self.load_player_has_stats()

        print("\n=== Done! ===")
        print("Your knowledge graph is now loaded in Neo4j.")
        print("Open Neo4j Browser and try: MATCH (n) RETURN n LIMIT 50")


def main():
    print("Football Knowledge Graph Loader")
    print("=" * 40)

    if not NEO4J_PASSWORD:
        print("Error: NEO4J_PASSWORD not set.")
        print("Create a .env file with your credentials (see .env.example)")
        return

    loader = Neo4jLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    try:
        loader.load_all()
    except Exception as e:
        print(f"\nError: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure Neo4j is running")
        print("2. Check NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD in the script")
        print("3. Install dependencies: pip install neo4j pandas")
    finally:
        loader.close()


if __name__ == "__main__":
    main()
