import pandas as pd
import pymssql
import pymysql
import os
import time
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import numpy as np
from matplotlib import cm
import math
import logging
from log_ttl_analys import log_ttl_report


logger = logging.getLogger('reporter')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
logger.info('Start')


def connect_sql():

	return pymssql.connect(
		server=os.environ.get('MSSQL_SERVER', ''),
		user=os.environ.get('MSSQL_LOGIN', ''),
		password=os.environ.get('MSSQL_PASSWORD', ''),
		database='voice_ai'			
	)


def send_photo_from_local_file_to_telegram(photo_path, caption = ''):
	try:
		token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
		chat_id = os.environ.get('TELEGRAM_CHAT', '')
		session = requests.Session()
		get_request = 'https://api.telegram.org/bot' + token
		get_request += '/sendPhoto?chat_id=' + chat_id
		if caption!='':
			get_request += '&caption=' + caption
		files = {'photo': open(photo_path, 'rb')}
		session.post(get_request, files=files)
	except Exception as e:
		logger.error('send_photo_from_local_file_to_telegram: '+str(e))


def send_text_to_telegram(text):
	try:
		token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
		chat_id = os.environ.get('TELEGRAM_CHAT', '')
		session = requests.Session()
		get_request = 'https://api.telegram.org/bot' + token
		get_request += '/sendMessage?chat_id=' + chat_id
		get_request += '&parse_mode=Markdown&text=' + text
		session.get(get_request)
	except Exception as e:
		logger.error('send_text_to_telegram: '+str(e))


def plot_grouped(df, header, tg_group):
	err = False
	try:
		df = df.drop(['base_name', '_merge', 'call_date'], axis=1)
		df = df.groupby(['day', 'ak', 'miko', 'mrm', 'incoming', 'outcoming']).count()
		for i in range(6):
			df.reset_index(level=0, inplace=True)
		df.incoming *= df.linkedid
		df.outcoming *= df.linkedid
		df.mrm *= df.linkedid
		df.miko *= df.linkedid
		df.ak *= df.linkedid

		grp = []
		tmp = df[['day', 'incoming']].groupby('day').sum()
		tmp.reset_index(level=0, inplace=True)
		grp.append(tmp)
		tmp = df[['day', 'outcoming']].groupby('day').sum()
		tmp.reset_index(level=0, inplace=True)
		grp.append(tmp)
		tmp = df[['day', 'ak']].groupby('day').sum()
		tmp.reset_index(level=0, inplace=True)
		grp.append(tmp)
		tmp = df[['day', 'miko']].groupby('day').sum()
		tmp.reset_index(level=0, inplace=True)
		grp.append(tmp)
		tmp = df[['day', 'mrm']].groupby('day').sum()
		tmp.reset_index(level=0, inplace=True)
		grp.append(tmp)
		tmp = df[['day', 'linkedid']].groupby('day').sum()
		tmp.reset_index(level=0, inplace=True)
		grp.append(tmp)

		df = grp[0]
		for i in range(5):
			df = df.merge(grp[i + 1], on='day', how='left')

		calls_max = df.linkedid.max() * 3
		# Decide Colors
		mycolors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:grey', 'red']

		# Draw Plot and Annotate
		fig, ax = plt.subplots(1, 1, figsize=(16, 9), dpi=80)
		columns = df.columns[1:6]
		labs = columns.values.tolist()

		# Prepare data
		x = df['day'].values.tolist()
		y0 = df[columns[0]].values.tolist()
		y1 = df[columns[1]].values.tolist()
		y2 = df[columns[2]].values.tolist()
		y3 = df[columns[3]].values.tolist()
		y4 = df[columns[4]].values.tolist()
		y = np.vstack([y0, y1, y2, y3, y4])

		# Plot for each column
		labs = columns.values.tolist()
		ax = plt.gca()
		ax.stackplot(x, y, labels=labs, colors=mycolors, alpha=0.8)

		# Decorations
		ax.set_title(header, fontsize=18)
		ax.set(ylim=[0, calls_max])
		ax.legend(fontsize=10, ncol=4)
		plt.xticks(x, rotation=60)
		plt.grid(alpha=0.5)

		# Lighten borders
		plt.gca().spines["top"].set_alpha(0)
		plt.gca().spines["bottom"].set_alpha(.3)
		plt.gca().spines["right"].set_alpha(0)
		plt.gca().spines["left"].set_alpha(.3)

		plt.savefig('report.png')

	except Exception as e:
		logger.error('plot_grouped: '+str(e))
		err = True

	if not err:
		send_photo_from_local_file_to_telegram('report.png')


def plot_lag(lag_df, header, columns):
	try:
		if len(lag_df):
			mycolors = ['tab:blue', 'tab:orange', 'tab:red']

			# Draw Plot and Annotate
			fig, ax = plt.subplots(1,1,figsize=(16, 9), dpi = 80)

			labs = columns.values.tolist()

			# Prepare data
			x  = lag_df['transcribation_hour'].values.tolist()
			y0 = lag_df[columns[0]].values.tolist()
			y1 = lag_df[columns[1]].values.tolist()
			y2 = lag_df[columns[1]].values.tolist()
			y = np.vstack([y0, y1, y2])

			# Plot for each column
			labs = columns.values.tolist()
			ax = plt.gca()
			ax.stackplot(x, y, labels=labs, colors=mycolors, alpha=0.8)

			# Decorations
			ax.set_title(header, fontsize=18)
			ax.legend(fontsize=10, ncol=4)
			#plt.xticks(x, rotation=60)
			plt.grid(alpha=0.5)
			plt.xlabel('Время события: Час')
			plt.ylabel('Длительность в минутах')

			# Lighten borders
			plt.gca().spines["top"].set_alpha(0)
			plt.gca().spines["bottom"].set_alpha(.3)
			plt.gca().spines["right"].set_alpha(0)
			plt.gca().spines["left"].set_alpha(.3)
			
			plt.savefig('report.png')
			send_photo_from_local_file_to_telegram('report.png')
	except Exception as e:
		logger.error('plot_lag: '+str(e))


def queue_tasks_report(trans_conn, source_id, header):
	try:
		today = datetime.datetime.now().date()
		yesterday = today - datetime.timedelta(days=1)
		date_from = yesterday.strftime('%Y.%m.%d %H:%M:%S')
		date_toto = today.strftime('%Y.%m.%d %H:%M:%S')
		query = "SELECT distinct"
		query += " cast('"+date_from+"' as datetime) as start_date,"
		query += " linkedid as linkedid_count,"
		query += " queue_date"
		query += " FROM transcribations"
		query += " WHERE transcribation_date > '"+date_from+"'"
		query += " and transcribation_date < '"+date_toto+"'"
		query += " and source_id = "+str(source_id)
		query += " and not queue_date is Null;"
		df = pd.read_sql(query, con = trans_conn)
		df['time'] = (df.queue_date - df.start_date)
		df.time = df.time.apply(lambda x: round(x.seconds/60))
		df = df.drop(['start_date', 'queue_date'], axis=1)
		df = df.groupby(df.time).count()
		df.reset_index(level=0, inplace=True)
		start_time = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
		(start_time + datetime.timedelta(minutes = 120)).time()
		df['queued'] = df.time.apply(lambda x: (start_time + datetime.timedelta(minutes = x)).time())
		if len(df):
			fig, ax = plt.subplots(figsize=(16,10), dpi= 80)    	
			sns.stripplot(df.queued, df.linkedid_count, jitter=0.25, size=8, ax=ax, linewidth=.5)
			plt.gca().set_xticklabels(labels = df.queued, rotation=30)
			# Decorations
			plt.grid(linestyle='--', alpha=0.5)
			plt.title(header, fontsize=22)
			# plt.show()
			plt.savefig('report.png')
			send_photo_from_local_file_to_telegram('report.png')
	except Exception as e:
		logger.error('queue_tasks_report: '+str(e))


def lag_df_prepare(df_in):
	try:
		df_in.drop(['duration','rq','qt','call','mrm','source_id', 'linkedid', 'source_id_sum', 'rt', 'rs', 'qs', 'sum_hour', 'call_sum', 'mrm_sum'], axis = 1, inplace = True)
		df_in = pd.DataFrame(df_in.groupby(['transcribation_hour']).median()/60)
		df_in['transcribation_hour'] = df_in.index
		return df_in
	except Exception as e:
		logger.error('lag_df_prepare: '+str(e))
		return None


def ranscribation_process_duration(trans_conn):
	try:
		today = datetime.datetime.now().date()
		yesterday = today - datetime.timedelta(days=1)
		date_from = yesterday.strftime('%Y.%m.%d %H:%M:%S')
		date_toto = today.strftime('%Y.%m.%d %H:%M:%S')
		
		# Transcribations
		query = "SELECT distinct"
		query += " linkedid,"
		query += " source_id,"
		query += " duration,"
		query += " DATEPART(HOUR, transcribation_date) as transcribation_hour,"
		query += " DATEDIFF(second, record_date, queue_date)-duration as rq,"
		query += " DATEDIFF(second, queue_date, transcribation_date) as qt,"
		query += " DATEDIFF(second, record_date, transcribation_date) as rt,"
		query += " CASE WHEN source_id = 1 then 1 else 0 end as call,"
		query += " CASE WHEN source_id = 2 then 1 else 0 end as mrm"
		query += " FROM transcribations"
		query += " WHERE transcribation_date > '"+date_from+"'"
		query += " and transcribation_date < '"+date_toto+"'"
		query += " and duration > 5"
		query += " and not queue_date is Null;"
		df_trans = pd.read_sql(query, con = trans_conn)

		# Summarization
		query = "SELECT distinct"
		query += " linkedid,"
		query += " source_id,"
		query += " DATEPART(HOUR, sum_date) as sum_hour,"
		query += " DATEDIFF(second, record_date, sum_date) as rs,"
		query += " CASE WHEN source_id = 1 then 1 else 0 end as call,"
		query += " CASE WHEN source_id = 2 then 1 else 0 end as mrm"
		query += " FROM summarization"
		query += " WHERE sum_date > '"+date_from+"'"
		query += " and sum_date < '"+date_toto+"'"
		#query += " and duration > 5"
		query += " and not sum_date is Null;"
		df_sum = pd.read_sql(query, con = trans_conn)
		
		df_ts = df_trans.merge(df_sum, how='left', left_on = ['linkedid'], right_on = ['linkedid'], suffixes=("", '_sum'))
		df_ts['qs'] = df_ts.rs-df_ts.rt

		df_call = pd.DataFrame(df_ts[df_ts.source_id==1])
		df_call['От записи до постановки в очередь']=df_call.rq*df_call.call
		df_call['От постановки в очередь до расшифровки']=df_call.qt*df_call.call
		df_call['От расшифровки до суммаризации']=df_call.qs*df_call.call
		df_call = lag_df_prepare(df_call)

		df_mrm = pd.DataFrame(df_ts[df_ts.source_id==2])
		df_mrm['От записи до постановки в очередь']=df_mrm.rq*df_mrm.mrm
		df_mrm['От постановки в очередь до расшифровки']=df_mrm.qt*df_mrm.mrm
		df_mrm['От расшифровки до суммаризации']=df_mrm.qs*df_mrm.mrm
		df_mrm = lag_df_prepare(df_mrm)

		plot_lag(df_call, 'Длительность транскрибации записей КЦ', df_call.columns[0:3])
		plot_lag(df_mrm, 'Длительность транскрибации записей МРМ', df_mrm.columns[0:3])
	except Exception as e:
		logger.error('ranscribation_process_duration: '+str(e))


def perfomance_by_processes(trans_conn):
	try:
		today = datetime.datetime.now().date()
		yesterday = today - datetime.timedelta(days=1)
		date_from = yesterday.strftime('%Y.%m.%d %H:%M:%S')
		date_toto = today.strftime('%Y.%m.%d %H:%M:%S')

		query = "select file_name, cpu, time, duration"
		query += " from perf_log "
		query += " where step = 2 and event_date > '"+date_from+"' and time > 5"
		query += " and event_date < '"+date_toto+"';"
		df = pd.read_sql(query, con = trans_conn)
		df['td'] = df.duration/df.time
		logger.info('perfomance_by_processes len(df): '+str(len(df)))
		if len(df):
			df.groupby('cpu').count().plot(
				y = ['file_name'], 
				kind="bar", 
				title='Транскрибация аудиофайлов за сутки'
			)		
			plt.xlabel("Процесс", fontsize = 14)
			plt.ylabel("Количество файлов", fontsize = 14)
			plt.savefig("report.png")
			send_photo_from_local_file_to_telegram('report.png')
			
			df.drop(['file_name', 'time', 'duration'], axis=1, inplace = True)
			df.groupby('cpu').mean().plot(
				y = ['td'], 
				kind="bar", 
				title='Средняя производительность транскрибации за сутки'
			)
			plt.xlabel("Процесс", fontsize = 14)
			plt.ylabel("Производительность", fontsize = 14)
			plt.savefig("report.png")
			send_photo_from_local_file_to_telegram('report.png')
	except Exception as e:
		logger.error('perfomance_by_processes: '+str(e))


def transcribation_summarization_count(trans_conn, days_count):
	try:
		today = datetime.datetime.now().date()
		yesterday = today - datetime.timedelta(days=days_count)
		date_from = yesterday.strftime('%Y.%m.%d %H:%M:%S')
		date_toto = today.strftime('%Y.%m.%d %H:%M:%S')

		query = "select distinct linkedid, record_date"
		query += " from transcribations"
		query += " where record_date > '"+date_from+"' and record_date < '"+date_toto+"';"
		trans = pd.read_sql(query, con = trans_conn)

		query = "select distinct linkedid, sum_date"
		query += " from summarization "
		query += " where record_date > '"+date_from+"' and record_date < '"+date_toto+"';"
		sums = pd.read_sql(query, con = trans_conn)

		df = trans.merge(sums, how = 'left', left_on = 'linkedid', right_on = 'linkedid')

		df['sum_date'] = df['sum_date'].dt.strftime('%Y-%m-%d')
		df['sum_date'] = pd.to_datetime(df['sum_date'])
		df['record_date'] = df['record_date'].dt.strftime('%Y-%m-%d')
		df['record_date'] = pd.to_datetime(df['record_date'])
		df.drop_duplicates(inplace=True)

		df['summarized'] = df.sum_date.apply(lambda x: 0 if pd.isnull(x) else 1)
		df['transcribed'] = np.ones(len(df))
		df.drop(['sum_date'], axis=1, inplace = True)
		df.drop_duplicates(inplace=True)
		
		df = df.groupby(['record_date']).sum()
		df.reset_index(level=0, inplace=True)
		df['record_date'] = df['record_date'].astype(str)

		# Prepare Data
		x = df['record_date'].values.tolist()
		y1 = df['summarized'].values.tolist()
		y2 = df['transcribed'].values.tolist()
		mycolors = ['tab:red', 'tab:blue']
		columns = ['Суммаризации', 'Транскрибации']

		# Draw Plot 
		fig, ax = plt.subplots(1, 1, figsize=(16,9), dpi= 80)
		ax.fill_between(x, y1=y1, y2=0, label=columns[0], alpha=0.5, color=mycolors[0], linewidth=2)
		ax.fill_between(x, y1=y2, y2=0, label=columns[1], alpha=0.5, color=mycolors[1], linewidth=2)	

		# Decorations
		ax.set_title('Количество транскрибаций и суммаризаций за 10 дней', fontsize=18)
		#ax.set(ylim=[0, 30])
		ax.legend(loc='center', fontsize=12)
		
		plt.savefig("report.png")
		send_photo_from_local_file_to_telegram('report.png')
	except Exception as e:
		logger.error('transcribation_summarization_count: '+str(e))


def calls_transcribations_relation(trans_conn):
	try:
		report =''
		# calls
		"""calls_conn = pymysql.connect(
			host = '10.2.4.87',
			user = 'root',
			passwd = 'root',
			database = 'ml'
		)
		calls_conn.cursor()"""

		# = = = connections report = = =
		seven_days = datetime.datetime.now().date() - datetime.timedelta(days=7)
		# date_from = seven_days.strftime('%Y:%m:%d %H:%M:%S')
		date_from = seven_days.strftime('%Y%m%d %H:%M:%S.000')
		date_toto = datetime.datetime.now().date().strftime('%Y%m%d %H:%M:%S.000')
		# calls
		query = "SELECT"
		# query += " date(call_date) as day,"
		# query += " date(call_date) as call_date,"
		query += " cast (call_date as date) as day,"
		query += " cast (call_date as date) as call_date,"
		query += " ak,"
		query += " miko,"
		query += " mrm,"
		query += " incoming,"
		# query += " not incoming as outcoming,"
		# query += " linkedid,"
		query += " CASE WHEN incoming = 1 THEN 0 ELSE 1 END as outcoming,"
		query += " REPLACE(linkedid, ' ', '') as linkedid,"
		query += " base_name"
		query += " from calls"
		query += " where cast (call_date as date)>='"+date_from+"'"
		query += " and cast (call_date as date)<'"+date_toto+"';"
		#calls = pd.read_sql(query, con=calls_conn)
		calls = pd.read_sql(query, con=trans_conn)
		date_min = calls.call_date.min()
		date_max = calls.call_date.max()

		date_from = datetime.datetime.strptime(str(date_min), '%Y-%m-%d').strftime('%Y%m%d %H:%M:%S.000')
		date_toto = datetime.datetime.strptime(str(date_max), '%Y-%m-%d').strftime('%Y%m%d %H:%M:%S.000')

		# transcribations
		query = "SELECT distinct cast(record_date as date) as day, linkedid from transcribations"
		query += " where cast(record_date as date)>='" + date_from + "' and cast(record_date as date)<='" + date_toto + "';"
		trans = pd.read_sql(query, con=trans_conn)

		df_all = pd.merge(calls, trans, on='linkedid', how="outer", indicator=True)
		df_all['day'] = df_all.day_x

		# remove tech records ++
		df_all.base_name = df_all.base_name.str.lower()
		tech = pd.merge(
			df_all[df_all.base_name == '1c_service'],
			df_all[df_all.base_name == '1c_service_spb'],
			on='linkedid', how="inner"
		)
		tech = pd.merge(
			tech,
			df_all[df_all.base_name == '1c_service_region'],
			on='linkedid', how="inner"
		)
		df_all = pd.DataFrame(df_all[~df_all.base_name.isnull()])
		df_all = df_all[~df_all.linkedid.isin(tech.linkedid.unique())].sort_values('linkedid')
		# remove tech records --
		report += 'Связь звонков и расшифровок за вчера:'

		yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
		report += '\nЗвонков: ' + str(len(calls[calls.day == yesterday].linkedid.unique()))
		report += '\nРасшифровок: ' + str(len(trans[trans.day == yesterday].linkedid.unique()))
		mask = (df_all._merge == 'both') & (df_all.day == yesterday)
		report += '\nСвязь установлена: ' + str(len(df_all[mask].linkedid.unique()))
		mask = (df_all._merge == 'left_only') & (df_all.day == yesterday)
		report += '\nИдентификатор расшифровки не найден среди звонков: ' + str(len(df_all[mask].linkedid.unique()))
		send_text_to_telegram(report)

		group = ''
		df = df_all[df_all._merge == 'both']
		plot_grouped(df, 'Соединение установлено', group)

		df = df_all[df_all._merge == 'left_only']
		df.to_csv('debug.csv') # ToDo: remove
		plot_grouped(df, 'Соединение не установлено', group)
	except Exception as e:
		logger.error('calls_transcribation_relation: '+str(e))


def sleep_until_time(hour, minute):
	now = datetime.datetime.now()
	if now.hour > hour or (now.hour == hour and now.minute >= minute):
		tomorrow = now + datetime.timedelta(days=1)
		tomorrow = tomorrow.replace(hour=hour, minute=minute, second=0, microsecond=0)
	else:
		tomorrow = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
	time.sleep((tomorrow - now).seconds)


def colorator(source_id):
	return 'red' if source_id == 1 else 'green'


def queue_time_vs_date(trans_conn):
	try:
		query = "select"
		query += " source_id,"
		query += " DATEDIFF(second, date, getdate()) as queued_seconds_from_now,"
		query += " DATEDIFF(second, record_date, getdate()) as recorded_seconds_from_now"
		query += " from queue order by record_date;"	
		queue = pd.read_sql(query, con = trans_conn)

		if len(queue):
		
			queue['color'] = queue['source_id'].apply(colorator)

			q_a = queue[queue.source_id == 1]
			q_b = queue[queue.source_id == 2]

			ratio = max(queue.queued_seconds_from_now) / max(queue.recorded_seconds_from_now) * 10
			fig, ax = plt.subplots(1, 1, figsize=(15, 10), dpi=80)

			ax.scatter(
				q_a.queued_seconds_from_now, 
				q_a.recorded_seconds_from_now, 
				color=q_a['color'],
				label="call",
					marker = 'x'
			)
			ax.scatter(
				q_b.queued_seconds_from_now, 
				q_b.recorded_seconds_from_now, 
				color=q_b['color'],
				label="mrm",
				marker = '.'
			)
			currentdate = datetime.datetime.today()
			currentdate = currentdate.strftime('%Y.%m.%d %H:%M:%S')
			ax.set_title('Очередь ' + currentdate, fontsize=18)

			# Set common labels
			ax.set_xlabel('Добавлено, сек. назад', fontsize=18)
			ax.set_ylabel('Записано, сек. назад', fontsize=18)

			plt.legend(bbox_to_anchor=(1, 1), loc='upper left', ncol=1)
			plt.savefig('queue.png')
			send_photo_from_local_file_to_telegram('queue.png')
	except Exception as e:
		logger.error('queue_time_vs_date: '+str(e))

def log_ttl():
	logger.info('log_ttl started')
	file_list = log_ttl_report(14)
	for log_ttl_plotting in file_list:
		report = f'Отчет по временным интервалам запросов МРМ за 14 дней\n{log_ttl_plotting}'
		logger.info(report)
		send_photo_from_local_file_to_telegram(log_ttl_plotting, report)
	logger.info('log_ttl finished')

def lost_call(trans_conn):
	try:
		today = datetime.datetime.now().date()
		yesterday = today - datetime.timedelta(days=1)
		date_from = yesterday.strftime('%Y.%m.%d %H:%M:%S')
		date_toto = today.strftime('%Y.%m.%d %H:%M:%S')
		query = "SELECT"
		query += " trans.linkedid as linkedid,"
		query += " max(trans.duration) as duration,"
		query += " max(trans.end_time) as end_time,"
		query += " max(trans.duration)-max(trans.end_time) as distance"
		query += " FROM transcribations as trans"
		query += " WHERE trans.record_date > '"+date_from+"'"
		query += " and trans.record_date < '"+date_toto+"'"
		query += " group by trans.linkedid"
		query += " having max(trans.duration)>30" 
		df_trans = pd.read_sql(query, con = trans_conn)

		mask_a = df_trans.distance>df_trans.duration/3
		mask_b = df_trans.distance<=df_trans.duration/3
		report = 'За период с \n'+date_from+' по\n'+date_toto+'\n'
		report += str(len(df_trans[mask_a]))
		report += ' звонков из '
		report += str(len(df_trans[mask_b]))
		report += ', или '
		report += str(math.ceil(100*len(df_trans[mask_a])/len(df_trans[mask_b])))
		report += '% звонков теряют более трети данных при транскрибации.\n'
		report += 'Из отчета исключены звонки, длительностью менее 30 секунд.'

		# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.plot.html
		# https://matplotlib.org/stable/tutorials/colors/colormaps.html

		cmap = cm.get_cmap('tab20c')
		plot = df_trans.plot(
			title='Звонки',
			xlabel='Длительность аудиозаписи',    
			ylabel='Длительность транскрибации',
			x='duration',
			y='end_time',
			c = df_trans.distance, 
			kind='scatter', 
			colormap=cmap, 
			figsize=(10,10),
			sharex=False
		)
		fig = plot.get_figure()
		fig.savefig('queue.png')
		send_photo_from_local_file_to_telegram('queue.png', report)
	except Exception as e:
		logger.error('lost_call: '+str(e))


def files_count():
	try:
		pathes = {
			'call':'audio/call/',
			'mrm':'audio/mrm/'
		}
		report = 'Stored audio files:'
		count_sum = 0
		for key in pathes:
			path = pathes[key]
			count = 0
			size_sum = 0
			for root, dirs, files in os.walk(path):
				for file in files:
					count += 1
					size = os.path.getsize(os.path.join(root, file))
					size_sum += size
			count_sum += count
			# format size as megabytes with spaces
			report += '\n\n=== '+key+':\nCount: '+str(count)+'\nSize: '+str(round(size_sum/1024/1024, 2))+' MB'
		if count_sum > 0:
			send_text_to_telegram(report)
	except Exception as e:
		logger.error('files_count: '+str(e))


def main():

	try:
		trans_conn = pymssql.connect(
				server = os.environ.get('MSSQL_SERVER', ''),
				user = os.environ.get('MSSQL_LOGIN', ''),
				password = os.environ.get('MSSQL_PASSWORD', ''),
				database = 'voice_ai'			
			)
	except Exception as e:
		logger.error('Error connecting to MSSQL: '+str(e))

	send_text_to_telegram('Voice monitoring started')
	
	while True:
		logger.info('Reports started')
		log_ttl()
		files_count()
		queue_time_vs_date(trans_conn)
		queue_tasks_report(trans_conn, 1, 'Поступление в очередь КЦ (количество linkedid в минуту)')		
		queue_tasks_report(trans_conn, 2, 'Поступление в очередь МРМ (количество linkedid в минуту)')
		ranscribation_process_duration(trans_conn)
		perfomance_by_processes(trans_conn)
		transcribation_summarization_count(trans_conn, days_count = 10)
		calls_transcribations_relation(trans_conn)
		lost_call(trans_conn)

		logger.info('Sleeping until 6:00')
		time.sleep(60)
		sleep_until_time(6, 0)


if __name__ == "__main__":
	main()
