import requests
import polars as pl

endpoints = dict(
    bootstrap_dynamic="https://draft.premierleague.com/api/bootstrap-dynamic",
    game="https://draft.premierleague.com/api/game",
    bootstrap_static="https://draft.premierleague.com/api/bootstrap-static",
    league_details="https://draft.premierleague.com/api/league/{League_ID}/details",
    league_element_status="https://draft.premierleague.com/api/league/{League_ID}/element-status",
    draft_league_trades="https://draft.premierleague.com/api/draft/league/{League_ID}/trades",
    draft_entry_transactions="https://draft.premierleague.com/api/draft/entry/{Team_ID}/transactions",
    pl_event_status="https://draft.premierleague.com/api/pl/event-status",
    event_live="https://draft.premierleague.com/api/event/{GW}/live",
    entry_public="https://draft.premierleague.com/api/entry/{Team_ID}/public",
    entry_my_team="https://draft.premierleague.com/api/entry/{Team_ID}/my-team",
    draft_choices="https://draft.premierleague.com/api/draft/{League_ID}/choices",
    watchlist="https://draft.premierleague.com/api/watchlist/{Team_ID}",
    entry_event="https://draft.premierleague.com/api/entry/{Team_ID}/event/{GW}"
)


# ---------------------------------------- BASICS ---------------------------------------- #
def get_data():
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    response = requests.get(url)
    return response.json()


def get_league_details(league_id):
    url = endpoints['league_details'].format(League_ID=league_id)
    response = requests.get(url).json()
    df = pl.from_records(response['league_entries'])
    df = df.with_columns(
    pl.col("joined_time").map_elements(
        lambda x: x[:10], return_dtype=pl.Utf8
        ).str.to_datetime("%Y-%m-%d")
    )
    df = df.drop(pl.col("id"))
    df = df.rename({"entry_id": "id"})

    return df


def get_all_players():
    bootstrap = requests.get(endpoints['bootstrap_static']).json()
    players = pl.from_records(bootstrap['elements']).select(pl.col(
        "id", "code", "web_name", "first_name", "second_name", "element_type"
    ))
    players = players.rename({"id": "element"})
    return players


# ----------------------------------------- STATS ---------------------------------------- #
def get_weekly_player_stats(gw):
    live = requests.get(endpoints['event_live'].format(GW=gw)).json()

    live = live['elements']
    stats = []
    for player_id, data in live.items():
        player_stats = data['stats']
        player_stats['element'] = int(player_id)
        stats.append(player_stats)
    stats_df = pl.from_records(stats)
    return stats_df


# ------------------------------------ LEAGUE SPECIFIC ----------------------------------- #
def get_team_picks(team_id, gw, filter_subs=True):
    id = team_id['id']
    team_name = team_id['entry_name']
    response = requests.get(endpoints['entry_event'].format(Team_ID=id, GW=gw)).json()
    picks = pl.from_records(response['picks'])
    picks = picks.with_columns(pl.lit(team_name).alias('team_name'))

    if filter_subs:
        subs = response['subs']
        for sub in subs:
            picks = picks.filter(pl.col("element") != sub['element_out'])
    return picks


def get_team_picks_for_all_teams(team_ids, gw, filter_subs=True):
    picks = [get_team_picks(team_id, gw, filter_subs) for team_id in team_ids]
    picks_df = pl.concat(picks)
    return picks_df


def get_trades(league_id):
    trades = requests.get(endpoints['draft_league_trades'].format(League_ID=league_id)).json()
    return trades['trades']


def get_transactions(team_ids, gw):
    for team_id in team_ids:
        id = team_id['id']
        team_name = team_id['entry_name']
    

def get_league_fixtures(league_id, gw):
    league_details = requests.get(endpoints['league_details'].format(League_ID=league_id)).json()
    entries = league_details['league_entries']
    entry_name_map = {entry['id']: entry['entry_name'] for entry in entries}

    fixtures = []
    for fixture in league_details['matches']:
        fixture['team_h_name'] = entry_name_map[fixture['league_entry_1']]
        fixture['team_a_name'] = entry_name_map[fixture['league_entry_2']]
        if fixture['event'] <= gw:
            fixtures.append(fixture)

    fixtures_df = pl.from_records(fixtures)
    fixtures_df = fixtures_df.select(pl.col('*').exclude(
        'finished', 'started', 'league_entry_1', 'league_entry_2', 
        'winning_method', 'winning_league_entry',
    ))

    fixtures_df = fixtures_df.rename({
        'league_entry_1_points': 'team_h_pts',
        'league_entry_2_points': 'team_a_pts',
    })

    # map winner
    fixtures_df = fixtures_df.with_columns(
        pl.when(pl.col('team_h_pts') > pl.col('team_a_pts'))
        .then(pl.col('team_h_name'))
        .when(pl.col('team_a_pts') > pl.col('team_h_pts'))
        .then(pl.col('team_a_name'))
        .otherwise(pl.lit('draw'))
        .alias('winner')
    )
    
    return fixtures_df


# ------------------------------------- FINAL REPORT ------------------------------------- #
def weekly_report_data(league_id, gw, filter_subs=True):
    # get league details
    league_teams = get_league_details(league_id)
    team_ids = league_teams.select(pl.col("entry_name", "id")).to_dicts()

    # Get transactions
    # TODO: need to authenticate to get this

    # get player info
    players = get_all_players()

    # Get picks and stats
    picks = get_team_picks_for_all_teams(team_ids, gw, filter_subs)
    stats = get_weekly_player_stats(gw)
    team_stats = picks.join(stats, on='element', how='left')
    team_stats = team_stats.join(players, on='element', how='left')

    team_stats = team_stats.select(pl.col('*').exclude(
        'is_captain', 'is_vice_captain', 'multiplier', 
        'influence', 'creativity', 'threat', 'ict_index'
    ))

    # Get fixtures
    fixtures = get_league_fixtures(league_id, gw)

    return team_stats, fixtures
