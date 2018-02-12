# import the required Python libraries
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import os
import requests
import time


class HockeyReference:

    def __init__(self, date_range_start, date_range_end):

        # initialize object variables
        self.date_range_start = date_range_start
        self.date_range_end = date_range_end
        self.df = None
        self.title = None
        self.url = None

    def daily_leaders_skaters(self):

        # give the object a title
        self.title = 'Skaters'

        # set the date to extract before entering the loop
        date_extract = self.date_range_start

        # set a flag to indicate whether the loop has completed at least once
        flag_loop = False

        while date_extract <= self.date_range_end:

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
                
                if flag_loop:
                    self.df = pd.concat([self.df, df_concat], axis=0)
                else:
                    self.df = df_concat
                    flag_loop = True
                    
            # increment the date to extract by one day and wait 3 seconds before continuing
            date_extract = date_extract + datetime.timedelta(days=1)
            time.sleep(3)

    def daily_leaders_goalies(self):

        # give the object a title
        self.title = 'Goalies'

        # set the date to extract before entering the loop
        date_extract = self.date_range_start

        # set a flag to indicate whether the loop has completed at least once
        flag_loop = False

        while date_extract <= self.date_range_end:

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

                if flag_loop:
                    self.df = pd.concat([self.df, df_concat], axis=0)
                else:
                    self.df = df_concat
                    flag_loop = True

            # increment the date to extract by one day and wait 3 seconds before continuing
            date_extract = date_extract + datetime.timedelta(days=1)
            time.sleep(3)

    def output_csv(self, dir_csv):

        # create a new directory if it does not already exist
        if not os.path.exists(dir_csv):
            os.mkdir(dir_csv)

        # write the DataFrame to a csv in the specified directory
        filename_csv = '{:%Y-%m-%d} {}.csv'.format(self.date_range_start, self.title)
        path_csv = os.path.join(dir_csv, filename_csv)
        self.df.to_csv(path_csv)


# directory of the output files
dir_csv = os.path.join(os.getcwd(), 'csv')

# define the range of dates to extract
date_range_start = datetime.datetime(2017, 10, 4)
date_range_end = datetime.datetime(2017, 10, 6)

s = HockeyReference(date_range_start, date_range_end)
s.daily_leaders_skaters()
s.output_csv(dir_csv)

g = HockeyReference(date_range_start, date_range_end)
g.daily_leaders_goalies()
g.output_csv(dir_csv)
