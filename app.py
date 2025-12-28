"""
Football Scout AI - FastAPI Backend
GraphRAG-powered football analytics platform
"""

import os
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List
from neo4j import GraphDatabase
from openai import OpenAI

# Load environment variables
load_dotenv()

# Import GraphRAG from Agent.py
from Agent import chain as graph_chain, analyze_transfer

# Initialize FastAPI
app = FastAPI(title="Football Scout AI")

# Neo4j connection
neo4j_driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)

# OpenRouter LLM client
llm_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY")
)

LLM_MODEL = "anthropic/claude-sonnet-4"


# ============== SofaScore Mappings ==============

def load_mappings():
    """Load SofaScore ID mappings from CSV files."""
    player_mapping = {}
    team_mapping = {}

    # Load player mapping
    try:
        df = pd.read_csv("tm_sofascore_mapping.csv")
        for _, row in df.iterrows():
            if pd.notna(row.get("sofascore_id")):
                player_mapping[int(row["tm_id"])] = int(float(row["sofascore_id"]))
        print(f"Loaded {len(player_mapping)} player mappings")
    except FileNotFoundError:
        print("Warning: tm_sofascore_mapping.csv not found")

    # Load team mapping
    try:
        df = pd.read_csv("tm_sofascore_team_mapping.csv")
        for _, row in df.iterrows():
            if pd.notna(row.get("sofascore_id")):
                team_mapping[int(row["tm_id"])] = int(float(row["sofascore_id"]))
        print(f"Loaded {len(team_mapping)} team mappings")
    except FileNotFoundError:
        print("Warning: tm_sofascore_team_mapping.csv not found")

    return player_mapping, team_mapping


PLAYER_SOFASCORE_MAP, TEAM_SOFASCORE_MAP = load_mappings()


def add_sofascore_id(player: dict) -> dict:
    """Add sofascore_id to a player dict if mapping exists."""
    if player.get("id"):
        player["sofascore_id"] = PLAYER_SOFASCORE_MAP.get(int(player["id"]))
    return player


def add_team_sofascore_id(team: dict) -> dict:
    """Add sofascore_id to a team dict if mapping exists."""
    if team.get("id"):
        team["sofascore_id"] = TEAM_SOFASCORE_MAP.get(int(team["id"]))
    return team


# ============== Pydantic Models ==============

class SearchFilters(BaseModel):
    position: Optional[str] = None
    nationality: Optional[str] = None
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    max_value: Optional[int] = None
    team: Optional[str] = None
    exclude_team: Optional[str] = None  # Exclude players from this team


class CompareRequest(BaseModel):
    players: List[dict]


class ScoutRequest(BaseModel):
    team: Optional[str] = None
    position: Optional[str] = None
    budget: Optional[int] = None
    priority: Optional[str] = None


class ChatRequest(BaseModel):
    message: str
    team_id: Optional[int] = None


# ============== Neo4j Helpers ==============

def run_query(query: str, params: dict = None):
    """Execute a Neo4j query and return results."""
    with neo4j_driver.session() as session:
        result = session.run(query, params or {})
        return [record.data() for record in result]


def get_llm_response(prompt: str, max_tokens: int = 1000) -> str:
    """Get response from LLM via OpenRouter."""
    try:
        response = llm_client.chat.completions.create(
            model=LLM_MODEL,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"Error: {str(e)}"


# ============== API Endpoints ==============

@app.get("/")
async def root():
    """Serve the main HTML page."""
    return FileResponse("templates/index.html")


@app.get("/api/filters")
async def get_filters():
    """Get available filter options (nationalities, teams)."""
    nationalities = run_query("""
        MATCH (p:Player)
        WHERE p.nationality IS NOT NULL AND p.nationality <> ''
        RETURN DISTINCT p.nationality AS nationality
        ORDER BY nationality
    """)

    teams = run_query("""
        MATCH (t:Team)
        WHERE t.name IS NOT NULL
        RETURN t.id AS id, t.name AS name
        ORDER BY name
    """)

    # Add sofascore_id to teams
    teams_with_ss = [add_team_sofascore_id(t) for t in teams]

    return {
        "nationalities": [n["nationality"] for n in nationalities],
        "teams": teams_with_ss
    }


@app.get("/api/team/{team_id}/squad")
async def get_team_squad(team_id: int):
    """Get all players in a team's squad for dropdown selection."""
    players = run_query("""
        MATCH (t:Team {id: $team_id})<-[:PLAYS_FOR]-(p:Player)
        OPTIONAL MATCH (p)-[:HAS_STATS]->(s:Stats)
        RETURN p.id AS id, p.name AS name, p.age AS age,
               p.nationality AS nationality, p.market_value AS market_value,
               p.preferred_positions AS position,
               s.total_goals AS goals, s.total_assists AS assists,
               s.total_matches AS matches
        ORDER BY p.name
    """, {"team_id": team_id})

    # Add sofascore_id to each player
    players_with_ss = [add_sofascore_id(p) for p in players]

    return {"players": players_with_ss}


@app.post("/api/players/search")
async def search_players(filters: SearchFilters):
    """Search players with filters."""
    conditions = []
    params = {}

    if filters.position:
        conditions.append("p.preferred_positions CONTAINS $position")
        params["position"] = filters.position

    if filters.nationality:
        conditions.append("p.nationality = $nationality")
        params["nationality"] = filters.nationality

    if filters.min_age is not None:
        conditions.append("p.age IS NOT NULL AND p.age >= $min_age")
        params["min_age"] = filters.min_age

    if filters.max_age is not None:
        conditions.append("p.age IS NOT NULL AND p.age <= $max_age")
        params["max_age"] = filters.max_age

    if filters.team:
        conditions.append("t.id = $team_id")
        params["team_id"] = int(filters.team)

    if filters.exclude_team:
        conditions.append("(t IS NULL OR t.id <> $exclude_team_id)")
        params["exclude_team_id"] = int(filters.exclude_team)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    query = f"""
        MATCH (p:Player)
        OPTIONAL MATCH (p)-[:PLAYS_FOR]->(t:Team)
        OPTIONAL MATCH (p)-[:HAS_STATS]->(s:Stats)
        {where_clause}
        RETURN p.id AS id, p.name AS name, p.age AS age,
               p.nationality AS nationality, p.market_value AS market_value,
               p.preferred_positions AS position, t.name AS team, t.id AS team_id,
               COALESCE(s.total_goals, 0) AS goals,
               COALESCE(s.total_assists, 0) AS assists,
               COALESCE(s.total_matches, 0) AS matches
        ORDER BY goals DESC
        LIMIT 50
    """

    players = run_query(query, params)

    # Filter by market value (needs parsing)
    if filters.max_value:
        def parse_value(v):
            if not v:
                return 0
            v = v.replace("€", "").replace(",", "")
            if "m" in v.lower():
                return float(v.lower().replace("m", "")) * 1000000
            if "k" in v.lower():
                return float(v.lower().replace("k", "")) * 1000
            return float(v) if v else 0

        players = [p for p in players if parse_value(p.get("market_value")) <= filters.max_value]

    # Add sofascore_id to each player
    players_with_ss = [add_sofascore_id(p) for p in players]

    return {"players": players_with_ss}


@app.post("/api/compare")
async def compare_players(request: CompareRequest):
    """Compare two players using AI."""
    if len(request.players) != 2:
        raise HTTPException(status_code=400, detail="Need exactly 2 players")

    p1, p2 = request.players

    context = f"""
    Player 1: {p1.get('name')}
    - Position: {p1.get('position')}
    - Age: {p1.get('age')}
    - Nationality: {p1.get('nationality')}
    - Market Value: {p1.get('market_value')}
    - Goals: {p1.get('goals', 0)}
    - Assists: {p1.get('assists', 0)}
    - Matches: {p1.get('matches', 0)}

    Player 2: {p2.get('name')}
    - Position: {p2.get('position')}
    - Age: {p2.get('age')}
    - Nationality: {p2.get('nationality')}
    - Market Value: {p2.get('market_value')}
    - Goals: {p2.get('goals', 0)}
    - Assists: {p2.get('assists', 0)}
    - Matches: {p2.get('matches', 0)}
    """

    prompt = f"""You are a football analyst helping a team manager decide on a transfer.

{context}

Compare these players and provide:
1. <strong>Statistical Comparison</strong>: Goals, assists, goal contributions per match
2. <strong>Value Analysis</strong>: Is the price fair for what you get?
3. <strong>Age & Potential</strong>: Who has more years ahead? Room to grow?
4. <strong>Recommendation</strong>: Which player would you sign and why?

Keep it concise (4-5 paragraphs). Use <strong> tags for headers."""

    analysis = get_llm_response(prompt)
    return {"analysis": analysis}


@app.post("/api/scout")
async def scout_players(request: ScoutRequest):
    """AI-powered transfer scouting."""
    # Get team context if provided
    team_context = ""
    if request.team:
        team_data = run_query("""
            MATCH (t:Team {id: $team_id})<-[:PLAYS_FOR]-(p:Player)
            OPTIONAL MATCH (p)-[:HAS_STATS]->(s:Stats)
            RETURN t.name AS team_name, collect({
                name: p.name,
                position: p.preferred_positions,
                age: p.age,
                goals: s.total_goals
            }) AS players
        """, {"team_id": int(request.team)})

        if team_data:
            team_context = f"Current squad of {team_data[0]['team_name']}:\n"
            for p in team_data[0]['players'][:10]:
                team_context += f"- {p['name']} ({p['position']}), Age: {p['age']}, Goals: {p.get('goals', 0)}\n"

    # Get candidate players (exclude current team)
    position_filter = ""
    if request.position:
        position_map = {
            "Goalkeeper": "Goalkeeper",
            "Defender": "Back",
            "Midfielder": "Midfield",
            "Forward": "Forward"
        }
        position_filter = f"AND p.preferred_positions CONTAINS '{position_map.get(request.position, request.position)}'"

    team_filter = ""
    if request.team:
        team_filter = f"AND (t IS NULL OR t.id <> {int(request.team)})"

    candidates = run_query(f"""
        MATCH (p:Player)-[:HAS_STATS]->(s:Stats)
        OPTIONAL MATCH (p)-[:PLAYS_FOR]->(t:Team)
        WHERE p.market_value IS NOT NULL {position_filter} {team_filter}
        RETURN p.name AS name, p.age AS age, p.nationality AS nationality,
               p.preferred_positions AS position, p.market_value AS market_value,
               s.total_goals AS goals, s.total_assists AS assists, t.name AS team
        ORDER BY s.total_goals DESC
        LIMIT 15
    """)

    candidates_text = "Available players in the market:\n"
    for c in candidates:
        candidates_text += f"- {c['name']} ({c['position']}), Age: {c['age']}, Team: {c.get('team', 'Free')}, Value: {c['market_value']}, Goals: {c['goals']}, Assists: {c['assists']}\n"

    priority_desc = {
        "goals": "prioritize goal scorers",
        "assists": "prioritize playmakers with good assists",
        "experience": "prioritize experienced players (older, more matches)",
        "potential": "prioritize young talent with growth potential"
    }

    prompt = f"""You are a football transfer scout advising a team manager.

{team_context}

{candidates_text}

Budget: Up to €{request.budget or 'unlimited'}
Priority: {priority_desc.get(request.priority, 'overall quality')}

Recommend the TOP 3 players to sign. For each:
1. <strong>Why they fit</strong>: How they improve the squad
2. <strong>Value assessment</strong>: Is the price fair?
3. <strong>Concerns</strong>: Any risks (age, form, etc.)

Use <strong> tags for player names. Be specific and concise."""

    recommendation = get_llm_response(prompt)
    return {"recommendation": recommendation}


@app.get("/api/team/{team_id}")
async def analyze_team(team_id: int):
    """Get team analysis."""
    # Get team stats
    team_data = run_query("""
        MATCH (t:Team {id: $team_id})<-[:PLAYS_FOR]-(p:Player)
        OPTIONAL MATCH (p)-[:HAS_STATS]->(s:Stats)
        RETURN t.name AS team_name, t.id AS team_id,
               count(p) AS player_count,
               avg(p.age) AS avg_age,
               sum(s.total_goals) AS total_goals,
               collect({
                   id: p.id,
                   name: p.name,
                   position: p.preferred_positions,
                   age: p.age,
                   nationality: p.nationality,
                   market_value: p.market_value,
                   goals: s.total_goals,
                   assists: s.total_assists,
                   matches: s.total_matches
               }) AS players
    """, {"team_id": team_id})

    if not team_data:
        raise HTTPException(status_code=404, detail="Team not found")

    data = team_data[0]

    # Calculate total value
    def parse_value(v):
        if not v:
            return 0
        v = str(v).replace("€", "").replace(",", "")
        if "m" in v.lower():
            return float(v.lower().replace("m", "")) * 1000000
        if "k" in v.lower():
            return float(v.lower().replace("k", "")) * 1000
        return 0

    total_value = sum(parse_value(p.get("market_value")) for p in data["players"])
    total_value_str = f"€{total_value/1000000:.1f}M" if total_value >= 1000000 else f"€{total_value/1000:.0f}k"

    # Add sofascore_id to each player
    players_with_ss = [add_sofascore_id(p) for p in data["players"]]

    # Get team sofascore_id
    team_sofascore_id = TEAM_SOFASCORE_MAP.get(team_id)

    # Generate AI analysis
    players_text = "\n".join([
        f"- {p['name']} ({p['position']}), Age: {p['age']}, Goals: {p.get('goals', 0)}, Assists: {p.get('assists', 0)}"
        for p in data["players"][:15]
    ])

    prompt = f"""Analyze this football squad:

Team: {data['team_name']}
Players: {data['player_count']}
Average Age: {data['avg_age']:.1f}
Total Goals: {data['total_goals']}

Squad:
{players_text}

Provide:
1. <strong>Squad Strengths</strong>: What positions are well-covered
2. <strong>Areas to Improve</strong>: Where reinforcements are needed
3. <strong>Transfer Priorities</strong>: Top 2-3 signing recommendations

Use <strong> for emphasis. Be concise (3 paragraphs)."""

    analysis = get_llm_response(prompt)

    return {
        "team_name": data["team_name"],
        "team_id": data["team_id"],
        "sofascore_id": team_sofascore_id,
        "player_count": data["player_count"],
        "avg_age": data["avg_age"] or 0,
        "total_value": total_value_str,
        "total_goals": data["total_goals"] or 0,
        "players": players_with_ss,
        "analysis": analysis
    }


@app.post("/api/chat")
async def chat(request: ChatRequest):
    """Chat with the AI using GraphRAG - can answer any football question."""
    try:
        # Add team context to the question if team_id is provided
        question = request.message
        if request.team_id:
            team_data = run_query("""
                MATCH (t:Team {id: $team_id})
                RETURN t.name AS name
            """, {"team_id": request.team_id})
            if team_data:
                question = f"[Context: User manages {team_data[0]['name']}] {request.message}"

        # Use GraphRAG to answer
        response = graph_chain.invoke({"query": question})
        return {"response": response["result"]}

    except Exception as e:
        # Fallback to direct LLM if GraphRAG fails
        print(f"GraphRAG error: {e}")

        # Get some basic context
        general = run_query("""
            MATCH (p:Player)
            OPTIONAL MATCH (p)-[:HAS_STATS]->(s:Stats)
            RETURN count(DISTINCT p) AS total_players,
                   avg(p.age) AS avg_age,
                   sum(s.total_goals) AS total_goals
        """)

        context = f"Database has {general[0]['total_players']} players."

        prompt = f"""You are a football analytics AI assistant.

{context}

User question: {request.message}

Provide a helpful response. If you need specific data, suggest what the user should ask for."""

        response = get_llm_response(prompt)
        return {"response": response}


# Proxy for SofaScore player images (to avoid CORS issues)
import httpx
from fastapi.responses import Response

@app.get("/api/player-image/{sofascore_id}")
async def get_player_image(sofascore_id: int):
    """Proxy SofaScore player images to avoid CORS."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://api.sofascore.com/api/v1/player/{sofascore_id}/image",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=5.0
            )
            if response.status_code == 200:
                return Response(
                    content=response.content,
                    media_type="image/png"
                )
    except:
        pass
    # Return empty/placeholder on error
    raise HTTPException(status_code=404, detail="Image not found")


@app.get("/api/team-image/{sofascore_id}")
async def get_team_image(sofascore_id: int):
    """Proxy SofaScore team images."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://api.sofascore.com/api/v1/team/{sofascore_id}/image",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=5.0
            )
            if response.status_code == 200:
                return Response(
                    content=response.content,
                    media_type="image/png"
                )
    except:
        pass
    raise HTTPException(status_code=404, detail="Image not found")


# Mount static files (create if needed)
try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except:
    pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
