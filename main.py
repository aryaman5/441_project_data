import pandas as pd
from nba_api.stats.endpoints import leaguegamelog

SEASON = "2024-25"
SEASON_TYPE = "Regular Season"

def fetch_nba_gamelog(season=SEASON, season_type=SEASON_TYPE) -> pd.DataFrame:
    print(f"Fetching NBA game logs for {season} ({season_type})...")
    gl = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star=season_type,
    )
    df = gl.get_data_frames()[0]
    print(f"Retrieved {len(df)} team-game rows.")
    return df

def build_game_level_with_stats(team_game_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert team-game logs (two rows per game: one per team) into a single
    row per game, with home/away teams and team stats for each game.
    """
    df = team_game_df.copy()
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])

    desired_stat_cols = [
        "FGM", "FGA", "FG_PCT",
        "FG3M", "FG3A", "FG3_PCT",
        "FTM", "FTA", "FT_PCT",
        "OREB", "DREB", "REB",
        "AST", "STL", "BLK",
        "TOV", "PF", "PTS",
        "PLUS_MINUS",
    ]
    available_stat_cols = [c for c in desired_stat_cols if c in df.columns]
    print("Using stat columns:", available_stat_cols)

    game_rows = []

    for game_id, g in df.groupby("GAME_ID"):
        if len(g) < 2:
            print(f"[WARN] GAME_ID {game_id} has only {len(g)} row(s); skipping.")
            continue

        game_date = g["GAME_DATE"].iloc[0]

        home_mask = g["MATCHUP"].str.contains("vs", case=False, na=False)
        away_mask = g["MATCHUP"].str.contains("@", case=False, na=False)

        num_home = home_mask.sum()
        num_away = away_mask.sum()

        if num_home == 1 and num_away == 1:
            home_row = g[home_mask].iloc[0]
            away_row = g[away_mask].iloc[0]
        else:
            print(
                f"[WARN] GAME_ID {game_id} has unexpected MATCHUP pattern: "
                f"{g['MATCHUP'].tolist()} (home_mask={num_home}, away_mask={num_away}) – skipping this game."
            )
            continue

        home_team = home_row["TEAM_ABBREVIATION"]
        away_team = away_row["TEAM_ABBREVIATION"]

        record = {
            "game_id": game_id,
            "game_date": game_date,
            "home_team": home_team,
            "away_team": away_team,
        }

        home_pts = home_row["PTS"]
        away_pts = away_row["PTS"]
        record["home_pts"] = home_pts
        record["away_pts"] = away_pts
        record["home_win"] = int(home_pts > away_pts)

        for col in available_stat_cols:
            record[f"home_{col.lower()}"] = home_row[col]
            record[f"away_{col.lower()}"] = away_row[col]

        game_rows.append(record)

    game_df = pd.DataFrame(game_rows)
    game_df = game_df.sort_values("game_date").reset_index(drop=True)
    return game_df

def main():
    team_game_df = fetch_nba_gamelog()
    game_with_stats_df = build_game_level_with_stats(team_game_df)

    print("Final shape:", game_with_stats_df.shape)
    print(game_with_stats_df.head())

    out_path = "nba_2024_2025_games_with_team_stats.csv"
    game_with_stats_df.to_csv(out_path, index=False)
    print(f"Saved dataset to {out_path}")

if __name__ == "__main__":
    main()
