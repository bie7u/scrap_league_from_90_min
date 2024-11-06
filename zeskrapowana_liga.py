import json
from bs4 import BeautifulSoup
import requests



def league_link():
    link_90_minut = input("Podaj link do ligi: ")
    return link_90_minut


def scrap_league(link):

    url = requests.get(f"{link}")

    soup = BeautifulSoup(url.content, "html.parser")

    league_name_to_find = soup.find_all("table", class_="main2")

    def league_name(league_name_list):
        league_name = ""
        for name in league_name_list:
            if not isinstance(name.find("td", class_="main"), type(None)):
                league_name = name.find("td", class_="main").text.strip()
        return league_name


    def league_name_json(league_name):
        return league_name.replace("/", "_").replace(" ", "_").replace(",", "").replace(":", "").lower()


    def json_load(league_name_json):
        try:
            with open(f"{league_name_json}.json", "r", encoding="utf-8") as input_file:
                table_json = json.load(input_file)

        except FileNotFoundError as e:
            table_json = {"Tabela": []}

        except json.decoder.JSONDecodeError as je:
            table_json = {"Tabela": []}
        return table_json


    def refresh_league(table, league_name):
        for slowniki_z_nazwami_lig in table["Tabela"]:
            for k_z_nazwa_ligi in slowniki_z_nazwami_lig:
                if league_name == k_z_nazwa_ligi:
                    del table["Tabela"][table["Tabela"].index(slowniki_z_nazwami_lig)]

    table_with_timetable_html = soup.find_all("table", class_="main")

    def data_league_table(soup):
        data_list = []
        p = soup.find_all("p")
        for i in p:
            if not isinstance(i.find("table", {"class": "main2"}), type(None)):
                data_list.append(i.find_all("table", {"class": "main2"}))
        return data_list[0]


    def teams_names(data_table):
        return [[i.text for i in info.find_all("a", class_="main")] for info in data_table][0]


    def data_to_table_league_list(data_html, teams_list):
        data_in_list = []
        teams_list = " ".join(teams_list).split()
        for i in data_html:
            teams = [t.text.split() for t in i.find_all("td")]
            for f in teams:
                for s in f:
                    if s not in teams_list and "(" not in s:
                        data_in_list.append(s)
        return data_in_list


    def rounds_list(data_html):
        round_td = []
        b_round_to_find = ""
        for td in data_html:
            round_td.append(td.find("td"))
        for b_round in round_td:
            if not isinstance(b_round.find("b"), type(None)) and "Kolejka" in b_round.find("b").text:
                b_round_to_find += f"{b_round.find("b").text},"
        b_round_to_find = b_round_to_find.split(",")[:-1]
        return b_round_to_find


    def timetable_not_divided(table_html, teams_list):
        data_tr = []
        full_timetable = []
        timetable = []
        for b_druzyn in table_html:
            data_tr.append(b_druzyn.find_all("tr"))

        for data_td in data_tr:
            for td in data_td:
                full_timetable.append([td.text.replace("\n", "").replace("\xa0", "")])

        for lists_i in full_timetable:
            for i in lists_i:
                for r in range(len(teams_list)):
                    if teams_list[r] in i and not "karnego" in i and not i in timetable:
                        timetable.append(i)
                if "Kolejka" in i:
                    timetable.append(i)
        return timetable


    def full_timetable(timetable_not_divided_list, rounds_in_list):
        indexes_round = []
        full_timetable_in_list = []
        attendence_to_remove_in_list = []
        timetable_in_stringu = ""
        full_timetable_without_attendence_in_list = []
        for i in rounds_in_list:
            indexes_round.append(timetable_not_divided_list.index(i))

        for r in range(1, len(indexes_round) + 1):
            try:
                full_timetable_in_list.append(timetable_not_divided_list[indexes_round[r - 1] + 1: indexes_round[r]])
            except IndexError:
                full_timetable_in_list.append(timetable_not_divided_list[indexes_round[r - 1] + 1:])

        for i_round in full_timetable_in_list:
            for i in i_round:
                try:
                   attendence_to_remove_in_list.append(f'({i.split(",")[1].split("(")[1]}')
                except IndexError:
                    pass

        for i in timetable_not_divided_list:
            i = i.replace(",", "")
            timetable_in_stringu += f"{i.strip(" ")}, "

        for r in range(len(attendence_to_remove_in_list)):
            timetable_in_stringu = timetable_in_stringu.replace(attendence_to_remove_in_list[r], "")

        full_timetable_in_list = timetable_in_stringu.split(",")

        for r in range(1, len(indexes_round) + 1):
            try:
                full_timetable_without_attendence_in_list.append(full_timetable_in_list[indexes_round[r - 1] + 1: indexes_round[r]])
            except IndexError:
                full_timetable_without_attendence_in_list.append(full_timetable_in_list[indexes_round[r - 1] + 1:])
        return full_timetable_without_attendence_in_list


    def save_table_in_json(table_json, data_table, teams_list, league_name):
        table_json["Tabela"].append({f"{league_name}": []})
        data_table[0:data_table.index('Nazwa')] = ""
        try:
            season_data = [data_table[data_table.index(f"{r + 1}."):data_table.index(f"{r + 1}.") + 15] for r in range(len(teams_list))]
        except ValueError as e:
            season_data = [data_table[data_table.index(f"{r + 1}."):data_table.index(f"{r + 1}.") + 15] for r in range(len(teams_list) - 1)]
            season_data.append(data_table[data_table.index(season_data[-1][-1]):data_table.index(season_data[-1][-1]) + 15])
            season_data[-1][0] = f"{len(season_data)}."
        table_info = data_table[:15]
        for r in range(len(season_data)):
            table_json["Tabela"][-1][league_name].append({"Pozycja": season_data[r][table_info.index('Nazwa')],
                "Nazwa": [i for i in teams_list][r],
                table_info[table_info.index('M.')]: season_data[r][table_info.index('M.')],
                table_info[table_info.index('Pkt.')]: season_data[r][table_info.index('Pkt.')],
                table_info[table_info.index('Z.')]: season_data[r][table_info.index('Z.')],
                table_info[table_info.index('R.')]: season_data[r][table_info.index('R.')],
                table_info[table_info.index('P.')]: season_data[r][table_info.index('P.')],
                table_info[table_info.index('Bramki')]: season_data[r][table_info.index('Bramki')],
                "Z.D.": season_data[r][table_info.index('Z.') + 4],
                "R.D.": season_data[r][table_info.index('R.') + 4],
                "P.D.": season_data[r][table_info.index('P.') + 4],
                "Bramki D.": season_data[r][table_info.index('Bramki') + 4],
                "Z.W.": season_data[r][table_info.index('Z.') + 8],
                "R.W.": season_data[r][table_info.index('R.') + 8],
                "P.W.": season_data[r][table_info.index('P.') + 8],
                "Bramki W.": season_data[r][table_info.index('Bramki') + 8]
                                    })


    def save_timetable_in_json(table_json, league_name, timetable,rounds):
        table_json["Tabela"][-1][league_name].append({"Terminarz": {}})
        for r in range(len(rounds)):
            table_json["Tabela"][-1][league_name][-1]['Terminarz'][rounds[r]] = timetable[r]


    def json_dump(table_json, league_name_json):
        with open(f"{league_name_json}.json", "w", encoding="utf-8") as output_file:
            json.dump(table_json, output_file, ensure_ascii=False, indent=4)


    def main():
        var_league_name = league_name(league_name_to_find)
        var_league_name_json = league_name_json(var_league_name)
        table_json = json_load(var_league_name_json)
        var = refresh_league(table_json, var_league_name)
        data_to_table_league_html = data_league_table(soup)
        teams_names_list = teams_names(data_to_table_league_html)
        data_table_list = data_to_table_league_list(data_to_table_league_html, teams_names_list)
        rounds_in_list = rounds_list(table_with_timetable_html)
        timetable_not_divided_list = timetable_not_divided(table_with_timetable_html, teams_names_list)
        full_timetable_var = full_timetable(timetable_not_divided_list, rounds_in_list)
        save_table_in_json(table_json, data_table_list, teams_names_list, var_league_name)
        save_timetable_in_json(table_json, var_league_name, full_timetable_var, rounds_in_list)
        json_dump(table_json, var_league_name_json)
        return var_league_name
    return main()


link = league_link()

skrapowanie = scrap_league(link)
