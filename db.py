import sqlite3 as sql

class Database:

	def __init__(self):
		self.lock = False
		self.conn = None

	def getConnection(self):
		if not self.lock:
			self.lock = True
			return self.conn
		return False

	def releaseConnection(self):
		if self.lock:
			self.lock = False
			print('[INFO]Releasing connection')
			return True
		return False

	def connect(self,dbURL):
		self.lock = False
		try:
			self.conn = sql.connect(dbURL)
			return True
		except e:
			print(e)
			return False

	def create_table(self,connection,query):
		if not connection:
			print("[INFO]Must create connection and initialize prior to creating table")
			return False

		# try:
		cursor = connection.cursor()
		print("[INFO]Creating table")
		cursor.execute(query)
		return True
		# except:
		# 	print("[ERROR]Cannot create table in database")
		# 	return False

	def insert_standings(self,connection,values):
		query = "INSERT INTO standings (year,division,team,mov,points_diff"
		query = query + ",srs_total,srs_offense,losses,points_opp,srs_defense"
		query = query + ",wins,sos_total,win_loss_perc,points,ties) "
		query = query + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

		try:
			cursor = connection.cursor()
			cursor.execute(query,values)
			# print("[INFO]Inserting data into the standings table")
			connection.commit()
			return True
		except:
			print('[ERROR]Cannot instert data into standings table')
			print(values)
			print()
			return False

	def insert_drive_avgs(self,connection,values):
		query = "INSERT INTO drive_averages (year,team,ranking"
		query = query + ",score_pct,start_avg,turnover_pct,drives"
		query = query + ",time_avg,play_count_tip,points_avg,yards_per_drive) "
		query = query + "VALUES(?,?,?,?,?,?,?,?,?,?,?);"

		try:
			cursor = connection.cursor()
			cursor.execute(query,values)
			# print("INFO Inserting data into the drive averages table")
			connection.commit()
			return True
		except:
			print('ERROR cannot insert into drive_averages')
			print(values)
			print()
			return False

	def insert_conversions(self,connection,values):
		query = "INSERT INTO conversions (year,team,ranking"
		query = query + ",third_down_att,fourth_down_att,fourth_down_pct,red_zone_scores"
		query = query + ",red_zone_pct,fourth_down_success,third_down_pct,red_zone_att,third_down_success) "
		query = query + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?);"

		try:
			cursor = connection.cursor()
			cursor.execute(query,values)
			# print("INFO Inserting data into the drive averages table")
			connection.commit()
			return True
		except:
			print('ERROR cannot insert into drive_averages')
			print(values)
			print()
			return False

	def insert_scoring_offense(self,connection,values):
		query = "INSERT INTO scoring_offense (year,team,ranking"
		query = query + ",points_per_g,xpm,fga,scoring,xpa,safety_md,otd,two_pt_att"
		query = query + ",fgm,two_pt_md,rushtd,prtd,frtd,krtd,alltd,rectd,ditd) "
		query = query + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

		try:
			cursor = connection.cursor()
			cursor.execute(query,values)
			# print("INFO Inserting data into the drive averages table")
			connection.commit()
			return True
		except:
			print('ERROR cannot insert into drive_averages')
			print(values)
			print()
			return False

	def insert_team_offense(self,connection,values):
		query = "INSERT INTO team_offense (year,team,ranking"
		query = query + ",pen_fd,turnovers,penalties,rush_td,total_yards"
		query = query + ",pass_fd,penalties_yds,pass_int,rush_fd,points"
		query = query + ",fumbles_lost,rush_att,pass_td,pass_cmp,rush_yds_per_att"
		query = query + ",pass_yds,pass_net_yds_per_att,score_pct,turnover_pct"
		query = query + ",rush_yds,first_down,exp_pts_tot,pass_att"
		query = query + ",yds_per_play_offense,plays_offense) "
		query = query + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?"
		query = query + ",?,?,?,?,?,?,?,?);"

		try:
			cursor = connection.cursor()
			cursor.execute(query,values)
			# print("INFO Inserting data into the drive averages table")
			connection.commit()
			return True
		except:
			print('ERROR cannot insert into drive_averages')
			print(values)
			print()
			return False

	def insert_kicking_punting_returns(self,connection,values):
		query = "INSERT INTO kicking_punting_returns (year,team,ranking"
		query = query + ",punt_ret,kick_ret,punt_ret_yds,all_purpose_yds"
		query = query + ",kick_ret_td,kick_ret_yds_per_ret,punt_ret_yds_per_ret"
		query = query + ",punt_ret_long,kick_ret_yds,kick_ret_long,punt_ret_td) "
		query = query + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

		try:
			cursor = connection.cursor()
			cursor.execute(query,values)
			# print("INFO Inserting data into the drive averages table")
			connection.commit()
			return True
		except:
			print('ERROR cannot insert into drive_averages')
			print(values)
			print()
			return False

	def insert_kicking_punting(self,connection,values):
		query = "INSERT INTO kicking_punting (year,team,ranking"
		query = query + ",fgm1,fgm2,fgm3,fgm4,fgm5,fga1,fga2,fga3,fga4"
		query = query + ",fga5,punt,punt_yds_per_punt,xp_perc,punt_long"
		query = query + ",punt_yds,fgm,xpa,xpm,fg_perc,punt_blocked,fga) "
		query = query + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

		try:
			cursor = connection.cursor()
			cursor.execute(query,values)
			# print("INFO Inserting data into the drive averages table")
			connection.commit()
			return True
		except:
			print('ERROR cannot insert into drive_averages')
			print(values)
			print()
			return False

	def insert_passing_offense(self,connection,values):
		query = "INSERT INTO passing_offense (year,team,ranking"
		query = query + ",comebacks,pass_adj_net_yds_per_att,pass_sacked_yds"
		query = query + ",pass_yds_per_cmp,pass_att,exp_pts_pass,pass_adj_yds_per_att"
		query = query + ",pass_sacked,pass_cmp_perc,pass_yds_per_g,pass_int_perc"
		query = query + ",pass_long,pass_td,pass_yds_per_att,pass_int,pass_cmp"
		query = query + ",pass_td_perc,pass_yds,gwd,pass_net_yds_per_att,pass_rating,pass_sacked_perc) "
		query = query + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

		try:
			cursor = connection.cursor()
			cursor.execute(query,values)
			# print("INFO Inserting data into the drive averages table")
			connection.commit()
			return True
		except:
			print('ERROR cannot insert into drive_averages')
			print(values)
			print()
			return False

	def insert_rushing_offense(self,connection,values):
		query = "INSERT INTO rushing_offense (year,team,ranking"
		query = query + ",rush_yds_per_att,rush_yds,rush_long"
		query = query + ",rush_yds_per_g,fumbles,rush_td,exp_pts_rush,rush_att) "
		query = query + "VALUES(?,?,?,?,?,?,?,?,?,?,?);"

		try:
			cursor = connection.cursor()
			cursor.execute(query,values)
			# print("INFO Inserting data into the drive averages table")
			connection.commit()
			return True
		except:
			print('ERROR cannot insert into drive_averages')
			print(values)
			print()
			return False

	def initialize(self,connection):
		if not connection:
			print("[INFO]No DB connection provided to initialize")
			return False

		standings = '''CREATE TABLE IF NOT EXISTS standings (
							id integer PRIMARY KEY AUTOINCREMENT,
							year integer NOT NULL,
							division text NOT NULL,
							team text NOT NULL,
							mov real NOT NULL,
							points_diff integer NOT NULL,
							srs_total real NOT NULL,
							srs_offense real NOT NULL,
							losses integer NOT NULL,
							points_opp integer NOT NULL,
							srs_defense real NOT NULL,
							wins integer NOT NULL,
							sos_total real NOT NULL,
							win_loss_perc real NOT NULL,
							points integer NOT NULL,
							ties integer DEFAULT 0);'''

		drive_avgs = '''CREATE TABLE IF NOT EXISTS drive_averages (
							id integer PRIMARY KEY AUTOINCREMENT,
							year integer NOT NULL,
							team text NOT NULL,
							ranking integer NOT NULL,
							score_pct real NOT NULL,
							start_avg text NOT NULL,
							turnover_pct real NOT NULL,
							drives integer NOT NULL,
							time_avg text NOT NULL,
							play_count_tip integer NOT NULL,
							points_avg real NOT NULL,
							yards_per_drive real NOT NULL);'''

		conversions = ''' CREATE TABLE IF NOT EXISTS conversions (
							id integer PRIMARY KEY AUTOINCREMENT,
							year integer NOT NULL,
							team text NOT NULL,
							ranking integer NOT NULL,
							third_down_pct real NOT NULL,
							third_down_success integer NOT NULL,
							red_zone_att integer NOT NULL,
							fourth_down_success integer NOT NULL,
							fourth_down_att integer NOT NULL,
							fourth_down_pct real NOT NULL,
							red_zone_pct real NOT NULL,
							red_zone_scores integer NOT NULL,
							third_down_att integer NOT NULL);'''

		scoring_offense = '''CREATE TABLE IF NOT EXISTS scoring_offense(
								id integer PRIMARY KEY AUTOINCREMENT,
								year integer NOT NULL,
								team text NOT NULL,
								ranking integer NOT NULL,
								points_per_g real NOT NULL,
								xpm integer NOT NULL,
								fga integer NOT NULL,
								scoring integer NOT NULL,
								xpa integer NOT NULL,
								safety_md integer NOT NULL,
								otd integer,
								two_pt_att integer NOT NULL,
								fgm integer NOT NULL,
								two_pt_md integer NOT NULL,
								rushtd integer NOT NULL,
								prtd integer,
								frtd integer,
								krtd integer,
								alltd integer NOT NULL,
								rectd integer NOT NULL,
								ditd integer);'''

		team_offense = '''CREATE TABLE IF NOT EXISTS team_offense(
							id integer PRIMARY KEY AUTOINCREMENT,
							year integer NOT NULL,
							team text NOT NULL,
							ranking integer NOT NULL,
							pen_fd integer NOT NULL,
							turnovers integer NOT NULL,
							penalties integer NOT NULL,
							rush_td integer NOT NULL,
							total_yards integer NOT NULL,
							pass_fd integer NOT NULL,
							penalties_yds integer NOT NULL,
							pass_int integer NOT NULL,
							rush_fd integer NOT NULL,
							points integer NOT NULL,
							fumbles_lost integer NOT NULL,
							rush_att integer NOT NULL,
							pass_td integer NOT NULL,
							pass_cmp integer NOT NULL,
							rush_yds_per_att real NOT NULL,
							pass_yds integer NOT NULL,
							pass_net_yds_per_att real NOT NULL,
							score_pct real NOT NULL,
							turnover_pct real NOT NULL,
							rush_yds integer NOT NULL,
							first_down integer NOT NULL,
							exp_pts_tot real NOT NULL,
							pass_att integer NOT NULL,
							yds_per_play_offense real NOT NULL,
							plays_offense integer NOT NULL);'''

		kicking_punting_returns = '''CREATE TABLE IF NOT EXISTS kicking_punting_returns(
										id integer PRIMARY KEY AUTOINCREMENT,
										year integer NOT NULL,
										team text NOT NULL,
										ranking integer NOT NULL,
										punt_ret integer NOT NULL,
										kick_ret integer NOT NULL,
										punt_ret_yds integer NOT NULL,
										all_purpose_yds integer NOT NULL,
										kick_ret_td integer DEFAULT 0,
										kick_ret_yds_per_ret real NOT NULL,
										punt_ret_yds_per_ret real NOT NULL,
										punt_ret_long integer NOT NULL,
										kick_ret_yds integer NOT NULL,
										kick_ret_long integer NOT NULL,
										punt_ret_td integer DEFAULT 0);'''

		kicking_punting = '''CREATE TABLE IF NOT EXISTS kicking_punting(
								id integer PRIMARY KEY AUTOINCREMENT,
								year integer NOT NULL,
								team text NOT NULL,
								ranking integer NOT NULL,
								fgm1 integer DEFAULT 0,
								fgm2 integer DEFAULT 0,
								fgm3 integer DEFAULT 0,
								fgm4 integer DEFAULT 0,
								fgm5 integer DEFAULT 0,
								fga1 integer DEFAULT 0,
								fga2 integer DEFAULT 0,
								fga3 integer DEFAULT 0,
								fga4 integer DEFAULT 0,
								fga5 integer DEFAULT 0,
								punt integer NOT NULL,
								punt_yds_per_punt real NOT NULL,
								xp_perc real NOT NULL,
								punt_long integer NOT NULL,
								punt_yds integer NOT NULL,
								fgm integer DEFAULT 0,
								xpa integer NOT NULL,
								xpm integer NOT NULL,
								fg_perc real NOT NULL,
								punt_blocked integer DEFAULT 0,
								fga integer NOT NULL);'''

		passing_offense = '''CREATE TABLE IF NOT EXISTS passing_offense(
								id integer PRIMARY KEY AUTOINCREMENT,
								year integer NOT NULL,
								team text NOT NULL,
								ranking integer NOT NULL,
								comebacks integer DEFAULT 0,
								pass_adj_net_yds_per_att real NOT NULL,
								pass_sacked_yds integer NOT NULL,
								pass_yds_per_cmp real NOT NULL,
								pass_att integer NOT NULL,
								exp_pts_pass real NOT NULL,
								pass_adj_yds_per_att real NOT NULL,
								pass_sacked integer NOT NULL,
								pass_cmp_perc real NOT NULL,
								pass_yds_per_g real NOT NULL,
								pass_int_perc real NOT NULL,
								pass_long integer NOT NULL,
								pass_td integer NOT NULL,
								pass_yds_per_att real NOT NULL,
								pass_int integer NOT NULL,
								pass_cmp integer NOT NULL,
								pass_td_perc real NOT NULL,
								pass_yds integer NOT NULL,
								gwd integer DEFAULT 0,
								pass_net_yds_per_att real NOT NULL,
								pass_rating real NOT NULL,
								pass_sacked_perc real NOT NULL);'''

		rushing_offense = '''CREATE TABLE IF NOT EXISTS rushing_offense(
								id integer PRIMARY KEY AUTOINCREMENT,
								year integer NOT NULL,
								team text NOT NULL,
								ranking integer NOT NULL,
								rush_yds_per_att real NOT NULL,
								rush_yds integer NOT NULL,
								rush_long integer NOT NULL,
								rush_yds_per_g real NOT NULL,
								fumbles integer NOT NULL,
								rush_td integer NOT NULL,
								exp_pts_rush real NOT NULL,
								rush_att integer NOT NULL);'''

		self.create_table(connection,standings)
		self.create_table(connection,drive_avgs)
		self.create_table(connection,conversions)
		self.create_table(connection,scoring_offense)
		self.create_table(connection,team_offense)
		self.create_table(connection,kicking_punting_returns)
		self.create_table(connection,kicking_punting)
		self.create_table(connection,passing_offense)
		self.create_table(connection,rushing_offense)

		return True

