#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import os
import pandas as pd
import datetime
import pandas.io.sql as psql
import pytz # timezone ("naive", "aware")
tz = pytz.utc

def scan_dbfile(path='./history'):
    files = os.listdir(path)
    results = []
    for filename in files:
        if filename[-3:] == '.db':
            fullpath = os.path.abspath(os.path.join(path, filename))
            results.append(fullpath)
    return results

def get_markets(dbname='database.db'):
    conn = sqlite3.connect(dbname)

    select_table = '''SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candles%' '''
    df = psql.read_sql_query(select_table, conn)
    name = df.name.str.split('_', expand=True)
    name["name"] = df["name"]
    exchange = os.path.basename(dbname).split("_")[0]
    name["exchange"] = exchange
    name["dbname"] = os.path.basename(dbname)
    name.columns = ["candles", "currency", "assets", "name", "exchange", "dbname"]
    df = df.merge(name)
    df = df.drop("candles", axis=1)
    df = df.rename(columns={'name':'table'})
    return df

def get_candle_range(dbname, tablename, fromdate, todate, what="*"):
    if isinstance(fromdate, datetime.datetime):
        fromdate = int(fromdate.timestamp())
    if isinstance(todate, datetime.datetime):
        todate = int(todate.timestamp())
    sql = """
      SELECT {what} from {tablename}
      WHERE start <= ? AND start >= ?
      ORDER BY start ASC
    """.format(what=what, tablename=tablename)
    conn = sqlite3.connect(dbname, params=[fromdate, todate])
    df = psql.read_sql_query(sql, conn)
    return df

def get_candle(dbname, tablename, what="*"):
    sql = """
      SELECT {what} from {tablename}
      ORDER BY start ASC
    """.format(what=what, tablename=tablename)
    conn = sqlite3.connect(dbname)
    df = psql.read_sql_query(sql, conn)
    return df

def get_all_candles():
    files = scan_dbfile('../gekko/history')
    print(files)
    columns = ['dbfile', 'table', 'exchange', 'currency', 'assets']
    tables = pd.DataFrame([], columns=columns)
    for f in files:
        markets = get_markets(dbname=f)
        tables = pd.concat([tables, markets])

    columns = ['id', 'start', 'open', 'high', 'low', 'close', 'vwp', 'volume', 'trades']
    candles = {}
    candles = pd.DataFrame([], columns=columns)
    for f in files:
        for (_, table) in tables.iterrows():
            tablename = table["table"]
            try:
                candle = get_candle(dbname=f, tablename=tablename)
            except:
                continue
            candle["table"] = tablename
            candles = pd.concat([candles, candle])
    candles["start"] = pd.to_datetime(candles["start"], unit='s')
    candles.index = candles["start"]
    return candles

if __name__ == '__main__':
    print(get_all_candles())
