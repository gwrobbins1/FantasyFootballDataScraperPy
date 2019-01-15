import sys
import configparser
import requests

from db import Database
from bs4 import BeautifulSoup, Comment

def loadConfig():
	path = "config.properties"
	config = configparser.ConfigParser()
	config.read(path)

	return config

def processCommentedTables(comments):
	processed_tables = []

	for comment in comments:
		commented_table = comment.find('table')
		if not commented_table == -1:
			commentSoup = BeautifulSoup(comment,'html.parser')
			commentTables = commentSoup.find_all('table',class_='stats_table')
			for commentTable in commentTables:
				processed_table = {}
				if commentTable.caption:
					table_name = commentTable.caption.text
					processed_table[table_name] = {}
					
					for row in commentTable.tbody.find_all('tr'):
						if row.th:
							ranking = row.th.text
							processed_table[table_name][ranking] = {}
	
						for data in row.find_all('td'):
							stat = data['data-stat']
							processed_table[table_name][ranking][stat] = data.text

					processed_tables.append(processed_table)

	return processed_tables

def processWrappedTables(wrappers):
	processed_tables = []

	for table in wrappers:
		processed_table = {}
		statsTable = table.find('table',class_='stats_table')

		if not statsTable == None:
			if statsTable.caption:
				table_name = statsTable.caption.text
				processed_table[table_name] = {}

				for row in table.table.tbody.find_all('tr'):
					if row.th:
						team = row.th.text
						processed_table[table_name][team] = {}

						for data in row.find_all('td'):
							stat = data['data-stat']
							processed_table[table_name][team][stat] = data.text

				processed_tables.append(processed_table)

	return processed_tables

# print team standings to console
def print_standings_table(tables):
	for table in tables:
		for name,stats_table in table.items():
			print(name)
			for team,stats_map in stats_table.items():
				print(team)
				for stat,data in stats_map.items():
					print(stat,end=':')
					print(data)
				print('//////////////////////////////////////')
		print('----------------------')
	return True

# most tables are commented out for some reason
# print those tables to console
def print_commented_tables(comment_tables):
	for table in comment_tables:
		for name,stats_table in table.items():
			print(name)
			# print(stats_table)
			for rank,team_stats in stats_table.items():
				print(rank)
				# print(team_stats)
				for stat,data in team_stats.items():
					print(stat,end=':')
					print(data)
				print('----------------------')
			print('/////////////////////////////////////')
	return True

def save_standings_table(year,tables,db):
	conn = db.getConnection()
	for table in tables:
		for name,stats_table in table.items():
			if name == 'AFC Standings Table':
				division = 'AFC'
			elif name == 'NFC Standings Table':
				division = 'NFC'
			else:
				print("[INFO]Unknown table processing standings")
				break
			for team,stats_map in stats_table.items():
				values = ()
				values = values + (year,)
				values = values + (division,)
				values = values + (team,)

				fields = {
					'mov':0,
					'points_diff':0,
					'srs_total':0,
					'srs_offense':0,
					'losses':0,
					'points_opp':0,
					'srs_defense':0,
					'wins':0,
					'sos_total':0,
					'win_loss_perc':0,
					'points':0,
					'ties':0
				}

				for stat,data in stats_map.items():
					if data:
						fields[stat] = data

				#order matters so manually add the stats
				values = values + (fields['mov'],)
				values = values + (fields['points_diff'],)
				values = values + (fields['srs_total'],)
				values = values + (fields['srs_offense'],)
				values = values + (fields['losses'],)
				values = values + (fields['points_opp'],)
				values = values + (fields['srs_defense'],)
				values = values + (fields['wins'],)
				values = values + (fields['sos_total'],)
				values = values + (fields['win_loss_perc'],)
				values = values + (fields['points'],)
				values = values + (fields['ties'],)

				if conn:
					db.insert_standings(conn,values)
					
	db.releaseConnection()
	return True

def save_drive_avgs_table(year,table,db):
	# print('INFO saving drive Averages')
	conn = db.getConnection()

	for rank,team_stats in table.items():
		fields = {
			'year':year,
			'team':"",
			'ranking':rank,
			'score_pct':0.0,
			'start_avg':"",
			'turnover_pct':0.0,
			'drives':0,
			'time_avg':"",
			'play_count_tip':0,
			'points_avg':0.0,
			'yards_per_drive':0.0
		}

		for stat,data in team_stats.items():
			if data:
				fields[stat] = data

		values = ()
		values = values + (fields['year'],)
		values = values + (fields['team'],)
		values = values + (fields['ranking'],)
		values = values + (fields['score_pct'],)
		values = values + (fields['start_avg'],)
		values = values + (fields['turnover_pct'],)
		values = values + (fields['drives'],)
		values = values + (fields['time_avg'],)
		values = values + (fields['play_count_tip'],)
		values = values + (fields['points_avg'],)
		values = values + (fields['yards_per_drive'],)
		
		# print("DEBUG saving:")
		# print(values)
		# print('------------------------------')
		db.insert_drive_avgs(conn,values)

	db.releaseConnection()
	return True

def save_conversions(year,table,db):
	# print('INFO saving conversions')
	conn = db.getConnection()

	for rank,team_stats in table.items():
		fields = {
			'year':year,
			'team':"",
			'ranking':rank,
			'third_down_att':0,
			'fourth_down_att':0,
			'fourth_down_pct':0.0,
			'red_zone_scores':0,
			'red_zone_pct':0,
			'fourth_down_success':0,
			'third_down_pct':0,
			'red_zone_att':0,
			'third_down_success':0
		}

		for stat,data in team_stats.items():
			if data:
				if '%' in data:
					data = float(data[:len(data)-1])
				fields[stat] = data

		values = ()
		values = values + (fields['year'],)
		values = values + (fields['team'],)
		values = values + (fields['ranking'],)
		values = values + (fields['third_down_att'],)
		values = values + (fields['fourth_down_att'],)
		values = values + (fields['fourth_down_pct'],)
		values = values + (fields['red_zone_scores'],)
		values = values + (fields['red_zone_pct'],)
		values = values + (fields['fourth_down_success'],)
		values = values + (fields['third_down_pct'],)
		values = values + (fields['red_zone_att'],)
		values = values + (fields['third_down_success'],)

		# print("DEBUG saving:")
		# print(values)
		# print('------------------------------')
		db.insert_conversions(conn,values)

	db.releaseConnection()
	return True

def save_scoring_offense(year,table,db):
	# print('INFO saving conversions')
	conn = db.getConnection()

	for rank,team_stats in table.items():
		fields = {
			'year':year,
			'team':"",
			'ranking':rank,
			'points_per_g':0.0,
			'xpm':0,
			'fga':0,
			'scoring':0,
			'xpa':0,
			'safety_md':0,
			'otd':0,
			'two_pt_att':0,
			'fgm':0,
			'two_pt_md':0,
			'rushtd':0,
			'prtd':0,
			'frtd':0,
			'krtd':0,
			'alltd':0,
			'rectd':0,
			'ditd':0
		}

		for stat,data in team_stats.items():
			if data:
				if '%' in data:
					data = float(data[:len(data)-1])
				fields[stat] = data

		values = ()
		values = values + (fields['year'],)
		values = values + (fields['team'],)
		values = values + (fields['ranking'],)
		values = values + (fields['points_per_g'],)
		values = values + (fields['xpm'],)
		values = values + (fields['fga'],)
		values = values + (fields['scoring'],)
		values = values + (fields['xpa'],)
		values = values + (fields['safety_md'],)
		values = values + (fields['otd'],)
		values = values + (fields['two_pt_att'],)
		values = values + (fields['fgm'],)
		values = values + (fields['two_pt_md'],)
		values = values + (fields['rushtd'],)
		values = values + (fields['prtd'],)
		values = values + (fields['frtd'],)
		values = values + (fields['krtd'],)
		values = values + (fields['alltd'],)
		values = values + (fields['rectd'],)
		values = values + (fields['ditd'],)

		# print("DEBUG saving:")
		# print(values)
		# print('------------------------------')
		db.insert_scoring_offense(conn,values)

	db.releaseConnection()
	return True

def save_team_offense(year,table,db):
	# print('INFO saving conversions')
	conn = db.getConnection()

	for rank,team_stats in table.items():
		fields = {
			'year':year,
			'team':"",
			'ranking':rank,
			'pen_fd':0,
			'turnovers':0,
			'penalties':0,
			'rush_td':0,
			'total_yards':0,
			'pass_fd':0,
			'penalties_yds':0,
			'pass_int':0,
			'rush_fd':0,
			'points':0,
			'fumbles_lost':0,
			'rush_att':0,
			'pass_td':0,
			'pass_cmp':0,
			'rush_yds_per_att':0.0,
			'pass_yds':0,
			'pass_net_yds_per_att':0.0,
			'score_pct':0.0,
			'turnover_pct':0.0,
			'rush_yds':0,
			'first_down':0,
			'exp_pts_tot':0.0,
			'pass_att':0,
			'yds_per_play_offense':0.0,
			'plays_offense':0
		}

		for stat,data in team_stats.items():
			if data:
				if '%' in data:
					data = float(data[:len(data)-1])
				fields[stat] = data

		values = ()
		values = values + (fields['year'],)
		values = values + (fields['team'],)
		values = values + (fields['ranking'],)
		values = values + (fields['pen_fd'],)
		values = values + (fields['turnovers'],)
		values = values + (fields['penalties'],)
		values = values + (fields['rush_td'],)
		values = values + (fields['total_yards'],)
		values = values + (fields['pass_fd'],)
		values = values + (fields['penalties_yds'],)
		values = values + (fields['pass_int'],)
		values = values + (fields['rush_fd'],)
		values = values + (fields['points'],)
		values = values + (fields['fumbles_lost'],)
		values = values + (fields['rush_att'],)
		values = values + (fields['pass_td'],)
		values = values + (fields['pass_cmp'],)
		values = values + (fields['rush_yds_per_att'],)
		values = values + (fields['pass_yds'],)
		values = values + (fields['pass_net_yds_per_att'],)
		values = values + (fields['score_pct'],)
		values = values + (fields['turnover_pct'],)
		values = values + (fields['rush_yds'],)
		values = values + (fields['first_down'],)
		values = values + (fields['exp_pts_tot'],)
		values = values + (fields['pass_att'],)
		values = values + (fields['yds_per_play_offense'],)
		values = values + (fields['plays_offense'],)

		# print("DEBUG saving:")
		# print(values)
		# print('------------------------------')
		db.insert_team_offense(conn,values)

	db.releaseConnection()
	return True

def save_passing_offense(year,table,db):
	# print('INFO saving conversions')
	conn = db.getConnection()

	for rank,team_stats in table.items():
		fields = {
			'year':year,
			'team':"",
			'ranking':rank,
			'comebacks':0,
			'pass_adj_net_yds_per_att':0.0,
			'pass_sacked_yds':0,
			'pass_yds_per_cmp':0.0,
			'pass_att':0,
			'exp_pts_pass':0.0,
			'pass_adj_yds_per_att':0.0,
			'pass_sacked':0,
			'pass_cmp_perc':0.0,
			'pass_yds_per_g':0.0,
			'pass_int_perc':0.0,
			'pass_long':0,
			'pass_td':0,
			'pass_yds_per_att':0.0,
			'pass_int':0,
			'pass_cmp':0,
			'pass_td_perc':0.0,
			'pass_yds':0,
			'gwd':0,
			'pass_net_yds_per_att':0.0,
			'pass_rating':0.0,
			'pass_sacked_perc':0.0
		}

		for stat,data in team_stats.items():
			if data:
				if '%' in data:
					data = float(data[:len(data)-1])
				fields[stat] = data

		values = ()
		values = values + (fields['year'],)
		values = values + (fields['team'],)
		values = values + (fields['ranking'],)
		values = values + (fields['comebacks'],)
		values = values + (fields['pass_adj_net_yds_per_att'],)
		values = values + (fields['pass_sacked_yds'],)
		values = values + (fields['pass_yds_per_cmp'],)
		values = values + (fields['pass_att'],)
		values = values + (fields['exp_pts_pass'],)
		values = values + (fields['pass_adj_yds_per_att'],)
		values = values + (fields['pass_sacked'],)
		values = values + (fields['pass_cmp_perc'],)
		values = values + (fields['pass_yds_per_g'],)
		values = values + (fields['pass_int_perc'],)
		values = values + (fields['pass_long'],)
		values = values + (fields['pass_td'],)
		values = values + (fields['pass_yds_per_att'],)
		values = values + (fields['pass_int'],)
		values = values + (fields['pass_cmp'],)
		values = values + (fields['pass_td_perc'],)
		values = values + (fields['pass_yds'],)
		values = values + (fields['gwd'],)
		values = values + (fields['pass_net_yds_per_att'],)
		values = values + (fields['pass_rating'],)
		values = values + (fields['pass_sacked_perc'],)

		# print("DEBUG saving:")
		# print(values)
		# print('------------------------------')
		db.insert_passing_offense(conn,values)

	db.releaseConnection()
	return True

def save_rushing_offense(year,table,db):
	# print('INFO saving conversions')
	conn = db.getConnection()

	for rank,team_stats in table.items():
		fields = {
			'year':year,
			'team':"",
			'ranking':rank,
			'rush_yds_per_att':0.0,
			'rush_yds':0,
			'rush_long':0,
			'rush_yds_per_g':0.0,
			'fumbles':0,
			'rush_td':0,
			'exp_pts_rush':0.0,
			'rush_att':0
		}

		for stat,data in team_stats.items():
			if data:
				if '%' in data:
					data = float(data[:len(data)-1])
				fields[stat] = data

		values = ()
		values = values + (fields['year'],)
		values = values + (fields['team'],)
		values = values + (fields['ranking'],)
		values = values + (fields['rush_yds_per_att'],)
		values = values + (fields['rush_yds'],)
		values = values + (fields['rush_long'],)
		values = values + (fields['rush_yds_per_g'],)
		values = values + (fields['fumbles'],)
		values = values + (fields['rush_td'],)
		values = values + (fields['exp_pts_rush'],)
		values = values + (fields['rush_att'],)

		# print("DEBUG saving:")
		# print(values)
		# print('------------------------------')
		db.insert_rushing_offense(conn,values)

	db.releaseConnection()
	return True

def save_kick_and_punt_returns(year,table,db):
	# print('INFO saving conversions')
	conn = db.getConnection()

	for rank,team_stats in table.items():
		fields = {
			'year':year,
			'team':"",
			'ranking':rank,
			'punt_ret':0,
			'kick_ret':0,
			'punt_ret_yds':0,
			'all_purpose_yds':0,
			'kick_ret_td':0,
			'kick_ret_yds_per_ret':0.0,
			'punt_ret_yds_per_ret':0.0,
			'punt_ret_long':0,
			'kick_ret_yds':0,
			'kick_ret_long':0,
			'punt_ret_td':0
		}

		for stat,data in team_stats.items():
			if data:
				if '%' in data:
					data = float(data[:len(data)-1])
				fields[stat] = data

		values = ()
		values = values + (fields['year'],)
		values = values + (fields['team'],)
		values = values + (fields['ranking'],)
		values = values + (fields['punt_ret'],)
		values = values + (fields['kick_ret'],)
		values = values + (fields['punt_ret_yds'],)
		values = values + (fields['all_purpose_yds'],)
		values = values + (fields['kick_ret_td'],)
		values = values + (fields['kick_ret_yds_per_ret'],)
		values = values + (fields['punt_ret_yds_per_ret'],)
		values = values + (fields['punt_ret_long'],)
		values = values + (fields['kick_ret_yds'],)
		values = values + (fields['kick_ret_long'],)
		values = values + (fields['punt_ret_td'],)

		# print("DEBUG saving:")
		# print(values)
		# print('------------------------------')
		db.insert_kicking_punting_returns(conn,values)

	db.releaseConnection()
	return True

def save_kicking_and_punting(year,table,db):
	# print('INFO saving conversions')
	conn = db.getConnection()

	for rank,team_stats in table.items():
		fields = {
			'year':year,
			'team':"",
			'ranking':rank,
			'fgm1':0,
			'fgm2':0,
			'fgm3':0,
			'fgm4':0,
			'fgm5':0,
			'fga1':0,
			'fga2':0,
			'fga3':0,
			'fga4':0,
			'fga5':0,
			'punt':0,
			'punt_yds_per_punt':0.0,
			'xp_perc':0.0,
			'punt_long':0,
			'punt_yds':0,
			'fgm':0,
			'xpa':0,
			'xpm':0,
			'fg_perc':0.0,
			'punt_blocked':0,
			'fga':0
		}

		for stat,data in team_stats.items():
			if data:
				if '%' in data:
					data = float(data[:len(data)-1])
				fields[stat] = data

		values = ()
		values = values + (fields['year'],)
		values = values + (fields['team'],)
		values = values + (fields['ranking'],)
		values = values + (fields['fgm1'],)
		values = values + (fields['fgm2'],)
		values = values + (fields['fgm3'],)
		values = values + (fields['fgm4'],)
		values = values + (fields['fgm5'],)
		values = values + (fields['fga1'],)
		values = values + (fields['fga2'],)
		values = values + (fields['fga3'],)
		values = values + (fields['fga4'],)
		values = values + (fields['fga5'],)
		values = values + (fields['punt'],)
		values = values + (fields['punt_yds_per_punt'],)
		values = values + (fields['xp_perc'],)
		values = values + (fields['punt_long'],)
		values = values + (fields['punt_yds'],)
		values = values + (fields['fgm'],)
		values = values + (fields['xpa'],)
		values = values + (fields['xpm'],)
		values = values + (fields['fg_perc'],)
		values = values + (fields['punt_blocked'],)
		values = values + (fields['fga'],)

		# print("DEBUG saving:")
		# print(values)
		# print('------------------------------')
		db.insert_kicking_punting(conn,values)

	db.releaseConnection()
	return True

def main():
	config = loadConfig()
	db = Database()

	# 2018 not included
	years = range(1990,2018)
	url_base = "https://www.pro-football-reference.com/years/"
	db.connect("nflTeamStats.db")
	conn = db.getConnection()
	if conn:
		db.initialize(conn)
		db.releaseConnection()
		conn = None

	save_functions = {
		'standings':save_standings_table,
		'Drive Averages Table':save_drive_avgs_table,
		'Conversions Table':save_conversions,
		'Scoring Offense Table':save_scoring_offense,
		'Team Offense Table':save_team_offense,
		'Passing Offense Table':save_passing_offense,
		'Rushing Offense Table':save_rushing_offense,
		'Kick & Punt Returns Table':save_kick_and_punt_returns,
		'Kicking & Punting Table':save_kicking_and_punting
	}

	for year in years:
		print('======================')
		print(year)
		print('======================')

		url = url_base + str(year) +'/'
		year_stats = requests.get(url)
		if year_stats.status_code == 200:
			soup = BeautifulSoup(year_stats.text,'html.parser')
			table_wrappers = soup.find_all('div',class_='table_wrapper')
			tables = processWrappedTables(table_wrappers)
			
			# most of the tables are commented out
			comments =  soup.findAll(text=lambda text:isinstance(text, Comment))
			comment_tables = processCommentedTables(comments)

			save_functions['standings'](year,tables,db)

			for table in comment_tables:
				for name,stats_table in table.items():
					if name in save_functions.keys():
						save_functions[name](year,stats_table,db)

if __name__ == '__main__':
	sys.exit(main())