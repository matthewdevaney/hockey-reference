# import the required Python libraries
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import os
import requests
import time


class HockeyReference:

    def __init__(self):

        # initialize object variables
        self.df = None
        self.title = None
        self.url = None

    def daily_activity_skaters(self, date_range_start, date_range_end):

        # give the object a title
        self.title = 'Skaters'

        # set the date to extract before entering the loop
        date_extract = date_range_start

        # set a flag to indicate whether the loop has completed at least once
        flag_loop = False

        while date_extract <= date_range_end:

            # generate the hockey-reference url
            stats_month = date_extract.strftime('%m')
            stats_day = date_extract.strftime('%d')
            stats_year = date_extract.strftime('%Y')
            self.url = 'https://www.hockey-reference.com/friv/dailyleaders.fcgi?month={}&day={}&year={}'.format(
                             stats_month, stats_day, stats_year)

            # create the soup object
            r = requests.get(self.url)
            html_doc = r.text
            soup = BeautifulSoup(html_doc, 'html.parser')

            # check if a skaters table exists (i.e. were games played on the date?)
            if soup.find(id='skaters'):

                # get all records from the skaters table
                skaters_table = soup.find(id='skaters').find_all('td')
                skaters_tbody = [cell.get_text() for cell in skaters_table]
                skaters_records = [tuple(skaters_tbody[row: row + 28]) for row in range(0, len(skaters_tbody), 28)]

                # make a list of player IDs
                player_id_list = []

                for player_id in range(len(skaters_table)):
                    try:
                        player_id_list.append(skaters_table[player_id]['data-append-csv'])
                    except:
                        pass

                # load the skaters data into a Pandas DataFrame
                columns_list = ['Player Name','Position','Team','Home Away','Opponent','Result','Boxscore','G','A','PTS','Plus Minus','PIM','EV','PP','SH','GW','EV','PP','SH','S','S%','SHFT','TOI','HIT','BLK','FOW','FOL','FO%']
                df_skaters = pd.DataFrame.from_records(skaters_records, columns=columns_list)
                df_skaters.insert(0, 'Date', date_extract)

                # concatenate the Player IDs to the skaters DataFrame
                df_player_id = pd.DataFrame(player_id_list, columns=['Player ID'])
                df_concat = pd.concat([df_player_id, df_skaters], axis=1)

                # delete the Boxscore column and replace the contents of the Home Away column
                del df_concat['Boxscore']
                df_concat['Home Away'] = df_concat['Home Away'].apply(lambda x: 'Home' if x == '' else 'Away')

                # index the skaters DataFrame by Date and Player ID and sort by the index
                df_concat.set_index(['Date','Player ID'],inplace=True, drop=True)
                df_concat.sort_index(axis=0, inplace=True, ascending=True)

                # append the current DataFrame to the object DataFrame
                if flag_loop:
                    self.df = pd.concat([self.df, df_concat], axis=0)
                else:
                    self.df = df_concat
                    flag_loop = True
                    
            # increment the date to extract by one day and wait 3 seconds before continuing
            date_extract = date_extract + datetime.timedelta(days=1)
            time.sleep(3)

    def daily_activity_goalies(self, date_range_start, date_range_end):

        # give the object a title
        self.title = 'Goalies'

        # set the date to extract before entering the loop
        date_extract = date_range_start

        # set a flag to indicate whether the loop has completed at least once
        flag_loop = False

        while date_extract <= date_range_end:

            # generate the hockey-reference url
            stats_month = date_extract.strftime('%m')
            stats_day = date_extract.strftime('%d')
            stats_year = date_extract.strftime('%Y')
            self.url = 'https://www.hockey-reference.com/friv/dailyleaders.fcgi?month={}&day={}&year={}'.format(
                stats_month, stats_day, stats_year)

            # create the soup object
            r = requests.get(self.url)
            html_doc = r.text
            soup = BeautifulSoup(html_doc, 'html.parser')

            # check if a skaters table exists (i.e. were games played on the date?)
            if soup.find(id='goalies'):

                # get all records from the skaters table
                goalies_table = soup.find(id='goalies').find_all('td')
                goalies_tbody = [cell.get_text() for cell in goalies_table]
                goalies_records = [tuple(goalies_tbody[row: row + 15]) for row in range(0, len(goalies_tbody), 15)]

                # make a list of player IDs
                player_id_list = []

                for player_id in range(len(goalies_table)):
                    try:
                        player_id_list.append(goalies_table[player_id]['data-append-csv'])
                    except:
                        pass

                # load the skaters data into a Pandas DataFrame
                columns_list = ['Player', 'Position','Team', 'Home Away', 'Opponent', 'Result', 'Boxscore', 'DEC',
                                'GA', 'SA', 'SV', 'SV%', 'SO', 'PIM', 'TOI']
                df_goalies = pd.DataFrame.from_records(goalies_records, columns=columns_list)
                df_goalies.insert(0, 'Date', date_extract)

                # concatenate the Player IDs to the skaters DataFrame
                df_player_id = pd.DataFrame(player_id_list, columns=['Player ID'])
                df_concat = pd.concat([df_player_id, df_goalies], axis=1)

                # delete the Boxscore column and replace the contents of the Home Away column
                del df_concat['Boxscore']
                df_concat['Home Away'] = df_concat['Home Away'].apply(lambda x: 'Home' if x == '' else 'Away')

                # index the skaters DataFrame by Date and Player ID and sort by the index
                df_concat.set_index(['Date', 'Player ID'], inplace=True, drop=True)
                df_concat.sort_index(axis=0, inplace=True, ascending=True)

                # append the current DataFrame to the object DataFrame
                if flag_loop:
                    self.df = pd.concat([self.df, df_concat], axis=0)
                else:
                    self.df = df_concat
                    flag_loop = True

            # increment the date to extract by one day and wait 3 seconds before continuing
            date_extract = date_extract + datetime.timedelta(days=1)
            time.sleep(3)

    def team_rosters(self, date_year):

        # give the object a title
        self.title = 'Rosters'

        # list of teams to scrape
        list_teams = ['ANA', 'ARI', 'BOS', 'BUF', 'CAR', 'CBJ', 'CGY', 'CHI', 'COL', 'DAL', 'DET', 'EDM', 'FLA', 'LAK',
                      'MIN', 'MTL', 'NJD', 'NSH', 'NYI', 'NYR', 'OTT', 'PHI', 'PIT', 'SJS', 'STL', 'TBL', 'TOR', 'VAN',
                      'VEG', 'WPG', 'WSH']

        # set a flag to indicate whether the loop has completed at least once
        flag_loop = False

        for team_id in list_teams:

            # generate the hockey reference url
            stats_webpage = 'https://www.hockey-reference.com/teams/{}/{}.html'.format(team_id, date_year)

            # create the soup object
            r = requests.get(stats_webpage)
            html_doc = r.text
            soup = BeautifulSoup(html_doc, 'html.parser')

            # get all records from the rosters table
            roster_table = soup.find(id='roster').find_all('td')
            roster_tbody = [cell.get_text() for cell in roster_table]
            roster_records = [tuple(roster_tbody[row: row + 12]) for row in range(0, len(roster_tbody), 12)]

            # make a list of player jersey numbers
            player_number_table = soup.find(id='roster').find_all('th', {'data-stat': 'number'})
            player_number_list = [cell.get_text() for cell in player_number_table]
            del player_number_list[0]

            # make a list of player IDs
            player_id_list = []

            for player_id in range(len(roster_table)):
                try:
                    player_id_list.append(roster_table[player_id]['data-append-csv'])
                except:
                    pass

            # load the rosters data into a Pandas DataFrame
            columns_list = ['Player', 'Flag', 'Position', 'Age', 'Height', 'Weight', 'Shoots/Catches',
                            'Years Experience', 'Birth Date', 'Summary', 'Salary', 'Draft']
            df_roster = pd.DataFrame.from_records(roster_records, columns=columns_list)

            # concatenate the Player IDs and jersey numbers to the skaters DataFrame
            df_player_number = pd.DataFrame(player_number_list, columns=['Number'])
            df_player_id = pd.DataFrame(player_id_list, columns=['Player ID'])
            df_concat = pd.concat([df_player_id, df_player_number, df_roster], axis=1)

            # delete the summary column
            del df_concat['Summary']

            # set the index to player ID
            df_concat.set_index('Player ID', drop=True, inplace=True)

            # add a column showing team name
            df_concat['Team'] = team_id

            # re-arrange the column positions
            df_concat = df_concat[['Player', 'Team', 'Number', 'Flag', 'Position', 'Age', 'Height', 'Weight', 'Shoots/Catches',
                                   'Years Experience', 'Birth Date', 'Salary', 'Draft']]

            # perform string operations on several columns
            df_concat['Flag'] = df_concat['Flag'].str.upper()
            df_concat['Salary'] = df_concat['Salary'].apply(lambda x: x.replace('$', '')).apply(
                lambda x: x.replace(',', ''))
            df_concat['Shoots/Catches'] = df_concat['Shoots/Catches'].apply(lambda x: 'L' if 'L' in x else 'R')

            # append the current DataFrame to the object DataFrame
            if flag_loop:
                self.df = pd.concat([self.df, df_concat], axis=0)
            else:
                self.df = df_concat
                flag_loop = True

            time.sleep(3)

    def output_csv(self, dir_csv):

        # create a new directory if it does not already exist
        if not os.path.exists(dir_csv):
            os.mkdir(dir_csv)

        # write the DataFrame to a csv in the specified directory
        filename_csv = '{:%Y-%m-%d} {}.csv'.format(date_range_start, self.title)
        path_csv = os.path.join(dir_csv, filename_csv)
        self.df.to_csv(path_csv)


# directory of the output files
dir_csv = os.path.join(os.getcwd(), 'csv')

# define the range of dates to extract
date_range_start = datetime.datetime(2017, 10, 5)
date_range_end = datetime.datetime(2017, 10, 6)
date_year = 2018

r = HockeyReference()
r.team_rosters(date_year)
r.output_csv(dir_csv)

"""
s = HockeyReference()
s.daily_activity_skaters(date_range_start, date_range_end)
s.output_csv(dir_csv)

g = HockeyReference()
g.daily_activity_goalies(date_range_start, date_range_end)
g.output_csv(dir_csv)
"""
