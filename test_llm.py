"""
Test script for LLM API connection via OpenRouter.
"""

import os
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# Initialize OpenRouter client
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY")
)


def test_connection():
    """Simple test to verify API connection works."""
    print("Testing OpenRouter API connection...")
    print("-" * 40)

    response = client.chat.completions.create(
        model="anthropic/claude-sonnet-4",
        max_tokens=200,
        messages=[
            {
                "role": "user",
                "content": "Say 'Hello! Claude API is working!' and nothing else."
            }
        ]
    )

    print(f"Response: {response.choices[0].message.content}")
    print("-" * 40)
    print("API connection successful!")


def test_football_query():
    """Test a football-related query to simulate GraphRAG."""
    print("\nTesting football analysis...")
    print("-" * 40)

    # Simulated context from Neo4j (we'll replace this with real data later)
    context = """
    Team: Raja Club Athletic
    League: Botola Pro
    Current Strikers:
    - Player A: Age 28, Goals: 12, Market Value: €500k
    - Player B: Age 31, Goals: 8, Market Value: €300k

    Available Strikers in League:
    - Oussama Lamlioui: Age 29, Goals: 60 total, Market Value: €900k
    - Paul Bassène: Age 24, Goals: 15 total, Market Value: €350k
    """

    response = client.chat.completions.create(
        model="anthropic/claude-sonnet-4",
        max_tokens=500,
        messages=[
            {
                "role": "user",
                "content": f"""You are a football transfer analyst.

CONTEXT:
{context}

QUESTION: Which striker should Raja Club Athletic sign and why?

Provide a brief recommendation based on the data."""
            }
        ]
    )

    print(f"Analysis:\n{response.choices[0].message.content}")
    print("-" * 40)


if __name__ == "__main__":
    api_key = os.getenv("OPENROUTER_API_KEY")

    if not api_key:
        print("Error: OPENROUTER_API_KEY not found in .env")
        print("Rename ANTHROPIC_API_KEY to OPENROUTER_API_KEY in your .env file")
        exit(1)

    print(f"API Key found: {api_key[:10]}...{api_key[-4:]}")
    print()

    test_connection()
    test_football_query()
