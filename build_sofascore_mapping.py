"""
Build a mapping table between Transfermarkt player IDs and SofaScore player IDs.
Uses local CSV files - no API calls needed.
"""

import pandas as pd
from difflib import SequenceMatcher
import unicodedata


def normalize_name(name: str) -> str:
    """Normalize player name for comparison"""
    if pd.isna(name):
        return ""
    # Remove accents
    name = unicodedata.normalize('NFKD', str(name)).encode('ASCII', 'ignore').decode('ASCII')
    # Lowercase and strip
    return name.lower().strip()


def similarity(a: str, b: str) -> float:
    """Calculate string similarity ratio"""
    return SequenceMatcher(None, normalize_name(a), normalize_name(b)).ratio()


def build_mapping():
    """Build mapping between Transfermarkt and SofaScore players"""

    # Load both datasets
    tm_players = pd.read_csv("tm_nodes/players.csv")
    ss_players = pd.read_csv("nodes/players.csv")

    print(f"Transfermarkt players: {len(tm_players)}")
    print(f"SofaScore players: {len(ss_players)}")
    print()

    # Create normalized name column for faster lookup
    ss_players["name_normalized"] = ss_players["name"].apply(normalize_name)

    results = []
    matched = 0
    unmatched = 0

    for idx, tm_row in tm_players.iterrows():
        tm_id = tm_row["id"]
        tm_name = tm_row["name"]
        tm_normalized = normalize_name(tm_name)

        # Find best match in SofaScore data
        best_match = None
        best_score = 0

        for _, ss_row in ss_players.iterrows():
            score = similarity(tm_name, ss_row["name"])

            if score > best_score:
                best_score = score
                best_match = ss_row

        # Threshold for accepting a match
        if best_match is not None and best_score >= 0.75:
            matched += 1
            results.append({
                "tm_id": tm_id,
                "tm_name": tm_name,
                "sofascore_id": best_match["id"],
                "sofascore_name": best_match["name"],
                "match_score": round(best_score, 2)
            })
            print(f"[MATCH] {tm_name} -> {best_match['name']} (score: {best_score:.2f})")
        else:
            unmatched += 1
            results.append({
                "tm_id": tm_id,
                "tm_name": tm_name,
                "sofascore_id": None,
                "sofascore_name": None,
                "match_score": round(best_score, 2) if best_match is not None else 0
            })
            if best_match is not None:
                print(f"[NO MATCH] {tm_name} (best: {best_match['name']}, score: {best_score:.2f})")
            else:
                print(f"[NO MATCH] {tm_name}")

    # Save results
    output_df = pd.DataFrame(results)
    output_df.to_csv("tm_sofascore_mapping.csv", index=False)

    print()
    print("=" * 50)
    print(f"COMPLETE!")
    print(f"  Matched:   {matched} ({matched/len(tm_players)*100:.1f}%)")
    print(f"  Unmatched: {unmatched}")
    print(f"  Saved to:  tm_sofascore_mapping.csv")
    print("=" * 50)

    return output_df


if __name__ == "__main__":
    print("=" * 50)
    print("  Player ID Mapping: Transfermarkt -> SofaScore")
    print("=" * 50)
    print()

    build_mapping()
