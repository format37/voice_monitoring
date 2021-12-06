import pymysql
import os
import pandas as pd
from aiohttp import web
from sqlalchemy import create_engine
import datetime
import re
from io import StringIO


def mysql_connector():
    mysql_server = os.environ.get('MYSQL_SERVER', '')
    mysql_user = os.environ.get('MYSQL_USER', '')
    mysql_pass = os.environ.get('MYSQL_PASS', '')
    return pymysql.connect(
        host = mysql_server,
        user = mysql_user,
        passwd = mysql_pass
    )    


def database_init():
    connector = mysql_connector()
    cursor = connector.cursor()
    db_name = 'ml'    
    cursor.execute('show databases')
    databases = cursor.fetchall()
    if not (db_name,) in databases:
        print('creating db: '+db_name)
        cursor.execute("CREATE DATABASE "+db_name)  

    cursor.execute('use '+db_name)

    query = "CREATE TABLE IF NOT EXISTS calls ("
    query += "    id INTEGER PRIMARY KEY,"
    query += "    call_date DATETIME,"
    query += "    ak BOOLEAN,"
    query += "    miko BOOLEAN,"
    query += "    mrm BOOLEAN,"
    query += "    incoming BOOLEAN,"
    query += "    linkedid CHAR(25),"
    query += "    base_name CHAR(25)"
    query += ")"
    cursor.execute(query)


async def call_test(request):
    content = "ok"
    return web.Response(text=content,content_type="text/html")


async def call_log(request):            
    try:
        # data -> df
        csv_text = str(await request.text()).replace('\ufeff', '')
        dateparser = lambda x: datetime.datetime.strptime(x, "%d.%m.%Y %H:%M:%S")
        df = pd.read_csv(
            StringIO(csv_text),
            sep=';',
            parse_dates=['call_date'],
            date_parser=dateparser
        )

        def get_base_name(val):
            return re.findall(r'"(.*?)"', val)[1]
        df.base_name = df.base_name.apply(get_base_name)
        df.linkedid = df.linkedid.str.replace('.WAV', '')

        # df -> mysql
        db_name = 'ml'
        mysql_server = os.environ.get('MYSQL_SERVER', '')+':3306'
        mysql_user = os.environ.get('MYSQL_USER', '')
        mysql_pass = os.environ.get('MYSQL_PASS', '')
        engine = create_engine(
            'mysql+pymysql://'+mysql_user+':' + mysql_pass + '@'+mysql_server+'/'+db_name,
            echo=False
        )
        df.to_sql(name='calls', con=engine, index=False, if_exists='append')
        answer = 'inserted: '+str(len(df))
    
    except Exception as e:
        answer = 'call_log error: '+str(e)

    return web.Response(text=answer, content_type="text/html")


def main():

	app = web.Application(client_max_size=1024**3)
	app.router.add_route('GET', '/test', call_test)
	app.router.add_route('POST', '/log', call_log)

	web.run_app(
		app,
		port=os.environ.get('PORT', ''),
	)


if __name__ == '__main__':
    main()
