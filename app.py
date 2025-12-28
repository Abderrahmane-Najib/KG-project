"""
Football Scout AI - FastAPI Backend
GraphRAG-powered football analytics platform
"""

import os
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


# ============== Pydantic Models ==============

class SearchFilters(BaseModel):
    position: Optional[str] = None
    nationality: Optional[str] = None
    min_age: Optional[int] = None
    max_age: Optional[int] = None
    max_value: Optional[int] = None
    team: Optional[str] = None


class CompareRequest(BaseModel):
    players: List[dict]


class ScoutRequest(BaseModel):
    team: Optional[str] = None
    position: Optional[str] = None
    budget: Optional[int] = None
    priority: Optional[str] = None


class ChatRequest(BaseModel):
    message: str


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

    return {
        "nationalities": [n["nationality"] for n in nationalities],
        "teams": teams
    }


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

    if filters.min_age:
        conditions.append("p.age >= $min_age")
        params["min_age"] = filters.min_age

    if filters.max_age:
        conditions.append("p.age <= $max_age")
        params["max_age"] = filters.max_age

    if filters.team:
        conditions.append("t.id = $team_id")
        params["team_id"] = int(filters.team)

    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

    query = f"""
        MATCH (p:Player)
        OPTIONAL MATCH (p)-[:PLAYS_FOR]->(t:Team)
        OPTIONAL MATCH (p)-[:HAS_STATS]->(s:Stats)
        {where_clause}
        RETURN p.id AS id, p.name AS name, p.age AS age,
               p.nationality AS nationality, p.market_value AS market_value,
               p.preferred_positions AS position, t.name AS team,
               s.total_goals AS goals, s.total_assists AS assists
        ORDER BY s.total_goals DESC
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

    return {"players": players}


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

    Player 2: {p2.get('name')}
    - Position: {p2.get('position')}
    - Age: {p2.get('age')}
    - Nationality: {p2.get('nationality')}
    - Market Value: {p2.get('market_value')}
    - Goals: {p2.get('goals', 0)}
    - Assists: {p2.get('assists', 0)}
    """

    prompt = f"""You are a football analyst. Compare these two players:

{context}

Provide a brief comparison covering:
1. Statistical comparison
2. Value for money
3. Who would you recommend and why

Keep it concise (3-4 paragraphs). Use <strong> tags for emphasis."""

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

    # Get candidate players
    position_filter = ""
    if request.position:
        position_map = {
            "Goalkeeper": "Goalkeeper",
            "Defender": "Back",
            "Midfielder": "Midfield",
            "Forward": "Forward"
        }
        position_filter = f"AND p.preferred_positions CONTAINS '{position_map.get(request.position, request.position)}'"

    candidates = run_query(f"""
        MATCH (p:Player)-[:HAS_STATS]->(s:Stats)
        WHERE p.market_value IS NOT NULL {position_filter}
        RETURN p.name AS name, p.age AS age, p.nationality AS nationality,
               p.preferred_positions AS position, p.market_value AS market_value,
               s.total_goals AS goals, s.total_assists AS assists
        ORDER BY s.total_goals DESC
        LIMIT 15
    """)

    candidates_text = "Available players:\n"
    for c in candidates:
        candidates_text += f"- {c['name']} ({c['position']}), Age: {c['age']}, Value: {c['market_value']}, Goals: {c['goals']}, Assists: {c['assists']}\n"

    priority_desc = {
        "goals": "prioritize goal scorers",
        "assists": "prioritize playmakers with good assists",
        "experience": "prioritize experienced players (older, more matches)",
        "potential": "prioritize young talent with growth potential"
    }

    prompt = f"""You are a football transfer scout.

{team_context}

{candidates_text}

Budget: Up to €{request.budget or 'unlimited'}
Priority: {priority_desc.get(request.priority, 'overall quality')}

Recommend the TOP 3 players to sign. For each:
1. Why they fit
2. Value assessment
3. Any concerns

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
        RETURN t.name AS team_name,
               count(p) AS player_count,
               avg(p.age) AS avg_age,
               sum(s.total_goals) AS total_goals,
               collect({
                   name: p.name,
                   position: p.preferred_positions,
                   age: p.age,
                   value: p.market_value,
                   goals: s.total_goals,
                   assists: s.total_assists
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

    total_value = sum(parse_value(p.get("value")) for p in data["players"])
    total_value_str = f"€{total_value/1000000:.1f}M" if total_value >= 1000000 else f"€{total_value/1000:.0f}k"

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
1. Squad strengths
2. Areas needing improvement
3. Transfer recommendations

Use <strong> for emphasis. Be concise (3 paragraphs)."""

    analysis = get_llm_response(prompt)

    return {
        "team_name": data["team_name"],
        "player_count": data["player_count"],
        "avg_age": data["avg_age"] or 0,
        "total_value": total_value_str,
        "total_goals": data["total_goals"] or 0,
        "players": data["players"],
        "analysis": analysis
    }


@app.post("/api/chat")
async def chat(request: ChatRequest):
    """Chat with the AI about football data."""
    user_message = request.message.lower()

    # Determine what data to retrieve
    context = ""

    # Check for team mentions
    teams = run_query("""
        MATCH (t:Team)
        RETURN t.id AS id, t.name AS name
    """)
    mentioned_team = None
    for team in teams:
        if team["name"].lower() in user_message:
            mentioned_team = team
            break

    if mentioned_team:
        team_info = run_query("""
            MATCH (t:Team {id: $team_id})<-[:PLAYS_FOR]-(p:Player)
            OPTIONAL MATCH (p)-[:HAS_STATS]->(s:Stats)
            RETURN t.name AS team,
                   collect({name: p.name, position: p.preferred_positions,
                           age: p.age, goals: s.total_goals, assists: s.total_assists})[..10] AS players
        """, {"team_id": mentioned_team["id"]})
        if team_info:
            context += f"\n{team_info[0]['team']} squad:\n"
            for p in team_info[0]["players"]:
                context += f"- {p['name']} ({p['position']}), Age: {p['age']}, Goals: {p.get('goals', 0)}\n"

    # Check for stats queries
    if any(word in user_message for word in ["top scorer", "most goals", "best striker"]):
        top_scorers = run_query("""
            MATCH (p:Player)-[:HAS_STATS]->(s:Stats)
            WHERE s.total_goals > 0
            RETURN p.name AS name, p.preferred_positions AS position,
                   s.total_goals AS goals, s.total_assists AS assists
            ORDER BY s.total_goals DESC
            LIMIT 10
        """)
        context += "\nTop scorers:\n"
        for i, p in enumerate(top_scorers, 1):
            context += f"{i}. {p['name']} - {p['goals']} goals, {p['assists']} assists\n"

    # Check for young player queries
    if any(word in user_message for word in ["young", "talent", "prospect", "under 23", "under 21"]):
        young_players = run_query("""
            MATCH (p:Player)-[:HAS_STATS]->(s:Stats)
            WHERE p.age <= 23 AND s.total_goals > 0
            RETURN p.name AS name, p.age AS age, p.preferred_positions AS position,
                   s.total_goals AS goals, p.market_value AS value
            ORDER BY s.total_goals DESC
            LIMIT 10
        """)
        context += "\nTop young players (U23):\n"
        for p in young_players:
            context += f"- {p['name']}, Age: {p['age']}, {p['position']}, Goals: {p['goals']}, Value: {p['value']}\n"

    # Default: get general stats
    if not context:
        general = run_query("""
            MATCH (p:Player)
            OPTIONAL MATCH (p)-[:HAS_STATS]->(s:Stats)
            RETURN count(DISTINCT p) AS total_players,
                   avg(p.age) AS avg_age,
                   sum(s.total_goals) AS total_goals
        """)
        if general:
            context += f"\nDatabase contains {general[0]['total_players']} players with average age {general[0]['avg_age']:.1f}\n"

    prompt = f"""You are a football analytics AI assistant with access to a knowledge graph of players, teams, and statistics.

DATA FROM KNOWLEDGE GRAPH:
{context}

USER QUESTION: {request.message}

Provide a helpful, accurate response based on the data above. If you don't have enough data to answer, say so.
Use conversational tone but be factual. Format nicely for readability."""

    response = get_llm_response(prompt)
    return {"response": response}


# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
