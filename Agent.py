from langchain_neo4j import Neo4jGraph, GraphCypherQAChain
from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from dotenv import load_dotenv
import os

load_dotenv()

# Connect to Neo4j
graph = Neo4jGraph(
    url=os.getenv("NEO4J_URI"),
    username=os.getenv("NEO4J_USER"),
    password=os.getenv("NEO4J_PASSWORD")
)

# Initialize Claude via OpenRouter
llm = ChatOpenAI(
    model="anthropic/claude-sonnet-4",
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1",
    temperature=0,
    max_tokens=1024
)

# Custom prompt for transfer analysis
CYPHER_PROMPT = PromptTemplate(
    input_variables=["schema", "question"],
    template="""You are a football transfer analyst expert. Generate a Cypher query to answer the question.

Schema:
{schema}

Key relationships:
- (Player)-[:PLAYS_FOR]->(Team)
- (Player)-[:HAS_STATS]->(Stats)
- (Player)-[:HAS_CONTRACT]->(Contract)
- (Team)-[:PARTICIPATES_IN]->(League)

Player properties: name, age, nationality, height, preferred_foot, preferred_positions, market_value
Stats properties: total_matches, total_goals, total_assists, total_yellow, goals_conceded, clean_sheets

For position searches, use CONTAINS on preferred_positions (e.g., "Centre-Forward", "Central Midfield", "Goalkeeper", "Defensive Midfield", "Left Winger", "Right-Back")

Market values are stored as strings like "€1.20m" or "€500k".

Question: {question}

Return relevant player data including stats for comparison. Always include name, age, market_value, and stats when comparing players.

Cypher Query:"""
)

QA_PROMPT = PromptTemplate(
    input_variables=["context", "question"],
    template="""You are a football transfer analyst. Based on the data, provide transfer recommendations and player analysis.

Data from database:
{context}

Question: {question}

Provide a clear analysis with:
1. Player recommendations ranked by fit
2. Key stats comparison (goals, assists, matches)
3. Value for money assessment (age vs market value)
4. Transfer recommendation with reasoning

Be specific and use the actual data provided. Format the response clearly.

Answer:"""
)

# Create GraphRAG chain with custom prompts
chain = GraphCypherQAChain.from_llm(
    llm=llm,
    graph=graph,
    cypher_prompt=CYPHER_PROMPT,
    qa_prompt=QA_PROMPT,
    verbose=True,
    allow_dangerous_requests=True,
    return_intermediate_steps=True
)


def analyze_transfer(question: str) -> str:
    """Run transfer analysis query"""
    try:
        response = chain.invoke({"query": question})
        return response["result"]
    except Exception as e:
        return f"Error: {e}"


def compare_players(player1: str, player2: str) -> str:
    """Compare two players"""
    query = f"Compare {player1} and {player2} - show their stats, age, market value, and recommend which is the better transfer option"
    return analyze_transfer(query)


def find_players_for_position(position: str, max_budget: str = None) -> str:
    """Find best players for a position"""
    query = f"Find the best players for {position} position"
    if max_budget:
        query += f" with market value under {max_budget}"
    query += ", rank them by goals and assists, include their age and current team"
    return analyze_transfer(query)


if __name__ == "__main__":
    print("\n" + "="*50)
    print("  FOOTBALL TRANSFER RECOMMENDATION SYSTEM")
    print("="*50)
    print("\nCommands:")
    print("  1. Ask any transfer question")
    print("  2. 'compare <player1> vs <player2>'")
    print("  3. 'find <position>' - e.g., 'find striker'")
    print("  4. 'quit' to exit")
    print("="*50 + "\n")

    while True:
        question = input("You: ").strip()

        if not question:
            continue
        if question.lower() == 'quit':
            print("Goodbye!")
            break

        # Handle specific commands
        if question.lower().startswith("compare "):
            parts = question[8:].split(" vs ")
            if len(parts) == 2:
                result = compare_players(parts[0].strip(), parts[1].strip())
            else:
                result = "Usage: compare <player1> vs <player2>"
        elif question.lower().startswith("find "):
            position = question[5:].strip()
            result = find_players_for_position(position)
        else:
            result = analyze_transfer(question)

        print(f"\nAnalyst: {result}\n")
