import pandas as pd
import pymssql
import os
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import requests
import numpy as np


def connect_sql():

	return pymssql.connect(
		server=os.environ.get('MSSQL_SERVER', ''),
		user=os.environ.get('MSSQL_LOGIN', ''),
		password=os.environ.get('MSSQL_PASSWORD', ''),
		database='voice_ai',
		#autocommit=True			
	)


def send_photo_from_local_file_to_telegram(photo_path):

	token = os.environ.get('TELEGRAM_BOT_TOKEN', '')
	chat_id = os.environ.get('TELEGRAM_CHAT', '')
	session = requests.Session()
	get_request = 'https://api.telegram.org/bot' + token
	get_request += '/sendPhoto?chat_id=' + chat_id
	files = {'photo': open(photo_path, 'rb')}
	session.post(get_request, files=files)


def plot_grouped(df, header, tg_group):

	df = df.drop(['base_name', '_merge', 'call_date'], axis=1)
	df = df.groupby(['day', 'ak', 'miko', 'mrm', 'incoming', 'outcoming']).count()
	# df.groupby('param')['group'].nunique()
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

	send_photo_from_local_file_to_telegram('report.png')


def plot_lag(df, header, columns):

	mycolors = ['tab:blue', 'tab:orange', 'red']

	# Draw Plot and Annotate
	fig, ax = plt.subplots(1,1,figsize=(16, 9), dpi = 80)

	labs = columns.values.tolist()

	# Prepare data
	x  = df['record_hour'].values.tolist()
	y0 = df[columns[0]].values.tolist()
	y1 = df[columns[1]].values.tolist()
	y = np.vstack([y0, y1])

	# Plot for each column
	labs = columns.values.tolist()
	ax = plt.gca()
	ax.stackplot(x, y, labels=labs, colors=mycolors, alpha=0.8)

	# Decorations
	ax.set_title(header, fontsize=18)
	ax.legend(fontsize=10, ncol=4)
	plt.grid(alpha=0.5)

	# Lighten borders
	plt.gca().spines["top"].set_alpha(0)
	plt.gca().spines["bottom"].set_alpha(.3)
	plt.gca().spines["right"].set_alpha(0)
	plt.gca().spines["left"].set_alpha(.3)

	# plt.show()
	plt.savefig('report.png')
	send_photo_from_local_file_to_telegram('report.png')


def queue_tasks_report(trans_conn, source_id, header):
		
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
	fig, ax = plt.subplots(figsize=(16,10), dpi= 80)    
	sns.stripplot(df.queued, df.linkedid_count, jitter=0.25, size=8, ax=ax, linewidth=.5)
	plt.gca().set_xticklabels(labels = df.queued, rotation=30)
	# Decorations
	plt.grid(linestyle='--', alpha=0.5)
	plt.title(header, fontsize=22)
	# plt.show()
	plt.savefig('report.png')
	send_photo_from_local_file_to_telegram('report.png')


def ranscribation_process_duration(trans_conn):
	
	today = datetime.datetime.now().date()
	yesterday = today - datetime.timedelta(days=1)
	date_from = yesterday.strftime('%Y.%m.%d %H:%M:%S')
	date_toto = today.strftime('%Y.%m.%d %H:%M:%S')
	query = "SELECT distinct"
	query += " DATEPART(HOUR, record_date) as record_hour,"
	query += " DATEDIFF(second,record_date, queue_date) as rq,"
	query += " DATEDIFF(second,queue_date, transcribation_date) as qt,"
	query += " CASE WHEN source_id = 1 then 1 else 0 end as call,"
	query += " CASE WHEN source_id = 2 then 1 else 0 end as mrm"
	query += " FROM transcribations"
	query += " WHERE transcribation_date > '"+date_from+"'"
	query += " and transcribation_date < '"+date_toto+"'"
	query += " and not queue_date is Null;"
	df = pd.read_sql(query, con = trans_conn)
	df['кц от записи до постановки в очередь']=df.rq*df.call
	df['кц от постановки в очередь до расшифровки']=df.qt*df.call
	df['мрм от записи до постановки в очередь']=df.rq*df.mrm
	df['мрм от постановки в очередь до расшифровки']=df.qt*df.mrm
	df.drop(['rq','qt','call','mrm'], axis = 1, inplace = True)
	df = pd.DataFrame(df.groupby(['record_hour']).median()/60/60)
	df['record_hour'] = df.index
	plot_lag(df, 'Длительность транскрибации записей КЦ (ч.) за сутки', df.columns[0:2])
	plot_lag(df, 'Длительность транскрибации записей МРМ (ч.) за сутки', df.columns[2:4])


def perfomance_by_processes(trans_conn):

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
	print(len(df))
	print(datetime.datetime.now())
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


def transcribation_summarization_count(trans_conn, days_count):

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


def sleep_until_time(hour, minute):
    now = datetime.datetime.now()
    if now.hour > hour or (now.hour == hour and now.minute >= minute):
        tomorrow = now + datetime.timedelta(days=1)
        tomorrow = tomorrow.replace(hour=hour, minute=minute, second=0, microsecond=0)
    else:
        tomorrow = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    time.sleep((tomorrow - now).seconds)


def main():

	trans_conn = pymssql.connect(
			server = os.environ.get('MSSQL_SERVER', ''),
			user = os.environ.get('MSSQL_LOGIN', ''),
			password = os.environ.get('MSSQL_PASSWORD', ''),
			database = 'voice_ai'			
		)

	trans_cursor = trans_conn.cursor()
	
	While True:
	
		sleep_until_time(7, 52)

		queue_tasks_report(trans_conn, 1, 'Поступление в очередь КЦ (количество linkedid в минуту)')		
		queue_tasks_report(trans_conn, 2, 'Поступление в очередь МРМ (количество linkedid в минуту)')
		ranscribation_process_duration(trans_conn)
		perfomance_by_processes(trans_conn)
		transcribation_summarization_count(trans_conn, days_count = 10)

		time.sleep(60)
	

if __name__ == "__main__":
	main()