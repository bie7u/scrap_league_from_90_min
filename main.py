import json
from types import NoneType

import psycopg2
from zeskrapowana_liga import skrapowanie

def save_sql(skrapowanie):
    with open(f"{skrapowanie.replace("/", "_").replace(" ", "_").replace(",", "").replace(":", "").lower()}.json", "r", encoding="utf-8") as f:
           tabela_json = json.load(f)


    def league_name():
        for k, v in tabela_json["Tabela"][-1].items():
            return k


    def teams_names():

        teams_names_list = []

        list_with_data_for_table = []

        for v in list(tabela_json["Tabela"][-1].values()):
            for i in v:
              list_with_data_for_table.append(i)

        for i in list_with_data_for_table:
            for k, v in i.items():
                if k == "Nazwa":
                    teams_names_list.append(v)

        return sorted(teams_names_list)


    def rounds():

        list_with_round_dict = []

        list_with_round = []

        for k, v in tabela_json["Tabela"][-1].items():
            for i in v:
                if list(i.keys())[0] == "Terminarz":
                    list_with_round_dict.append(list(i.values())[0])

        for i in list_with_round_dict:
            for k, v in i.items():
                list_with_round.append(k)

        return list_with_round


    def pg_connect():

        con = psycopg2.connect(
                host = 'localhost',
                database = 'dbtest',
                user = 'postgres',
                port = "5432",
                password = 'buber2006')

        con.autocommit = True

        return con


    create_table_clubs_names = """CREATE TABLE IF NOT EXISTS clubs_names (
                        league_name VARCHAR(255) NOT NULL,
                        club_id serial PRIMARY KEY,
                        club_name VARCHAR(255) NOT NULL
                        );"""
    create_table_timetable ="""CREATE TABLE IF NOT EXISTS "timetable"(
                     league_name VARCHAR(255) NOT NULL,
                     id SERIAL PRIMARY KEY,
                     round_number INT,
                     date_round VARCHAR(255) DEFAULT NULL,
                     home_team_id INT,
                     CONSTRAINT FK_Home_team FOREIGN KEY (home_team_id) REFERENCES "clubs_names"(club_id),
                     team_one_goals INT DEFAULT NULL,
                     away_team_id INT,
                     CONSTRAINT FK_AWAY_team FOREIGN KEY (away_team_id) REFERENCES "clubs_names"(club_id),
                     team_two_goals INT DEFAULT NULL,
                     home_team_win BOOLEAN,
                     draw BOOLEAN,
                     away_team_win BOOLEAN
                 );"""
    insert_into_clubs_names = """INSERT INTO "clubs_names"
                                (league_name, club_name) SELECT %s, %s
                                 WHERE (SELECT COUNT(*) FROM clubs_names WHERE league_name = %s) < %s;"""
    insert_into_timetable = """INSERT INTO timetable
    (league_name, round_number, date_round, home_team_id, team_one_goals, away_team_id, team_two_goals, home_team_win, draw, away_team_win)
    SELECT {league_name}, {round_number}, {date}, {first_select},
    {team_one_goals}, {second_select},
    {team_two_goals}, {home_team_win}, {draw}, {away_team_win} WHERE (SELECT COUNT(*) FROM timetable WHERE league_name = {league_name3}) < {len_league_name};"""

    select_club_id = """(SELECT club_id FROM clubs_names WHERE club_name = '{club_name}' AND league_name = '{league_name}')"""
    update_timetable = """UPDATE timetable
                          SET date_round = {date},
                              team_one_goals = {team_one_goals},
                              team_two_goals = {team_two_goals}
                          WHERE league_name = '{league_name}' and team_one_goals = null or team_two_goals = null;"""
    update_timetable_t_o_f = """UPDATE timetable
                                SET draw = false
                                WHERE team_one_goals = NULL or team_two_goals = NULL"""

    def full_timetable(league_name, teams_names):
        timetable = []
        for league in tabela_json["Tabela"]:
            for k, v in league.items():
                if k == league_name:
                    timetable = league[league_name][len(teams_names):][0]
        return timetable


    def insert_clubs_names(pg, pg_qr, teams_names,league_name):
        for dane_druzyn in teams_names:
            pg.execute(pg_qr, (league_name, dane_druzyn, league_name, len(teams_names)))
        return pg_qr


    def results(rounds, full_timetable, teams_names):

        results_list = []
        all_months = ['lipca', 'sierpnia', 'września', 'października', 'listopada', 'grudnia', 'stycznia', 'lutego', 'marca', 'kwietnia', 'maja', 'czerwca']

        for r in range(len(rounds)):
            for i in full_timetable["Terminarz"][rounds[r]]:
                if ":" in i:
                    i = i.split()[:-3]
                    for i1 in i:
                        if i1 not in " ".join(teams_names) or i1 == '-' or i1[1] == '-':
                            if i1 == '-' or i1[1] == '-' and " ".join(i[:i.index(i1)]) in " ".join(teams_names) and " ".join(i[i.index(i1) + 1:]) in " ".join(teams_names):
                                results_list.append({rounds[r]:" ".join(i).replace(f"{i1} ", f"~{i1}~").split("~")})

                else:

                    if '-' in i.split():
                        results_list.append({rounds[r]: [" ".join(i.split()[:i.split().index("-")]), '-', " ".join(i.split()[i.split().index("-") + 1:])]})

                    else:

                        try:
                            if not i.split()[-1] in all_months:
                                for i1 in i.split():
                                    if i1[1] == "-" or i1[0] == "-" :
                                        results_list.append({rounds[r]:i.replace(f"{i1} ", f"~{i1}~").split("~")})

                            else:
                                match_day = " ".join(i.split()[-2:])
                                print(match_day)
                                for i1 in i.split():
                                    if i1[1] == "-" or i1[0] == "-" :
                                        results_list.append({rounds[r]:i.replace(f"{i1} ", f"~{i1}~").replace(match_day, "").split("~")})

                        except IndexError:
                            pass
        return results_list


    def save_in_pg(results, rounds, pg, pg_gr, pg_select, pg_update, league_name):
        for r in range(len(rounds)):
            for i in results:
                try:
                    team_one_goals = int(i[rounds[r]][1].split("-")[0]) if i[rounds[r]][1].split("-")[0] != '' else 'null'
                    team_two_goals = int(i[rounds[r]][1].split("-")[1]) if i[rounds[r]][1].split("-")[1] != '' else 'null'
                    date_round = rounds[r].split()[3:]
                    home_team = pg_select.format(club_name=f"{i[rounds[r]][0].strip()}", league_name=f"{league_name}")
                    away_team = pg_select.format(club_name=f"{i[rounds[r]][2].strip()}", league_name=f"{league_name}")
                    pg.execute(pg_gr.format(league_name=f"'{league_name}'",
                                       round_number=rounds[r].split()[1],
                                       date=f"'{" ".join(date_round)}'", first_select=home_team,
                                       team_one_goals=team_one_goals, second_select=away_team, team_two_goals=team_two_goals,
                                       home_team_win=(True if team_one_goals > team_two_goals else False)
                                       if not isinstance(team_one_goals, type(None)) and not isinstance(team_two_goals, type(None)) else False,
                                       draw=((True if team_one_goals == team_two_goals else False)
                                       if not isinstance(team_one_goals, str) and not isinstance(team_two_goals, str)
                                             else False) if not isinstance(team_one_goals, type(None)) and not isinstance(team_two_goals, type(None)) else False,
                                       away_team_win=(True if team_one_goals < team_two_goals else False)
                                       if not isinstance(team_one_goals, type(None)) and not isinstance(team_two_goals, type(None)) else False,
                                       league_name3=f"'{league_name}'", len_league_name=len(results)))
                    pg.execute(pg_update.format(date=f"'{" ".join(date_round)}'", team_one_goals=team_one_goals,
                                                team_two_goals=team_two_goals, league_name=f"{league_name}"))
                except KeyError:
                    continue


    def main():
        var_league_name = league_name()
        var_teams_names = teams_names()
        var_rounds = rounds()
        pg_connect_var = pg_connect().cursor()
        pg_connect_var.execute(create_table_clubs_names)
        pg_connect_var.execute(create_table_timetable)
        var_full_timetable = full_timetable(var_league_name, var_teams_names)
        var_i_c_n = insert_clubs_names(pg_connect_var, insert_into_clubs_names, var_teams_names, var_league_name)
        var_results = results(var_rounds, var_full_timetable, var_teams_names)
        var_save_in_pg = save_in_pg(var_results, var_rounds, pg_connect_var, insert_into_timetable,
                                    select_club_id, update_timetable, var_league_name)
        pg_connect_var.execute(update_timetable_t_o_f)
        pg_connect_var.close()
    return main()

save_sql(skrapowanie)
