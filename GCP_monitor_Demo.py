import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def check_mirror(table: str, source_db_type: str, source_db: str, source_db_schema: str,
                 bq_schema: str) -> tuple:  # Bigquery資料處理方式為整筆重抄
    # 判斷來源資料庫
    time_start = (datetime.now() - timedelta(days=1)).date()
    if source_db_type == "source_mssql_db":
        engine = create_engine(f'''mssql_db_url''')
    elif source_db_type == "postgresql":
        engine = create_engine(f"""postgresql_db_url""")
    source_df = pd.read_sql(f'''SELECT count(*) FROM {source_db_schema}.{table}''', con=engine)
    source = source_df.values[0][0]
    # 連線Bigquery
    bq_engine = create_engine('bigquery://project')
    target_df = pd.read_sql(f"""SELECT count(*) FROM `bigquery_project.{bq_schema}.{table.lower()}`""",
                            con=bq_engine)
    target = target_df.values[0][0]
    # 寫入在地Postgresql log
    record_session.execute(
        f"""INSERT INTO schema.gcp_monitor(date, table_name, source, target) VALUES ('{time_start}', '{table.lower()}', '{source}', '{target}')ON CONFLICT (date, table_name) DO NOTHING""")
    return source, target


def check_append(table: str, source_db_type: str, source_db: str, source_db_schema: str, bq_schema: str,
                 search_keyword: str) -> tuple:  # Bigquery資料處理方式為資料合併
    # 判斷來源資料庫
    time_start = (datetime.now() - timedelta(days=1)).date()
    time_end = (datetime.now() - timedelta(days=0)).date()
    if source_db_type == "source_mssql_db":
        engine = create_engine(f'''mssql_db_url''')
    elif source_db_type == "postgresql":
        engine = create_engine(f"""postgresql_db_url""")
    source_df = pd.read_sql(
        f"""SELECT count(*) FROM {source_db_schema}.{table} WHERE "{search_keyword}" >= '{time_start}' AND "{search_keyword}" < '{time_end}'""",
        con=engine)
    source = source_df.values[0][0]
    # 連線Bigquery
    bq_engine = create_engine('bigquery_project_url')
    target_df = pd.read_sql(
        f"""SELECT count(*) FROM `project.{bq_schema}.{table.lower()}` WHERE {search_keyword} >= '{time_start}' AND {search_keyword} < '{time_end}'""",
        con=bq_engine)
    target = target_df.values[0][0]
    # 寫入在地Postgresql log
    record_session.execute(
        f"""INSERT INTO schema.gcp_monitor(date, table_name, source, target) VALUES ('{time_start}', '{table.lower()}', '{source}', '{target}')ON CONFLICT (date, table_name) DO NOTHING""")
    return source, target


def get_bq_table_list(schema: str) -> np.ndarray:
    bq_engine = create_engine('bigquery_project_url')
    bigquery = f"""SELECT table_name FROM `project`.{schema}.INFORMATION_SCHEMA.TABLES"""
    table_list = pd.read_sql(bigquery, con=bq_engine)['table_name'].to_numpy()
    return table_list


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'GCP_key位置'  # GCP_key位置
record_engine = create_engine(f"""postgresql_record_url""")
Session = sessionmaker(bind=record_engine)
record_session = Session()
NG_text = str()
exception_text = str()
now_date = datetime.now().date()
NG_text += f"""日期：{str(now_date)}\n檢查資料日期：{str(now_date - timedelta(days=1))}\nGCP Monitor check result：\n"""  # NG_text初始數字共58字
exception_text += f"""Tables don't exist in GCP：\n"""  # exception_text初始數字共27字
df = pd.read_csv("./DB_organization.csv")
db_list = df.fillna(np.NaN).to_numpy()
# 取得Bigquery相對應schema的table名稱列表
schema_1_list = get_bq_table_list("schema_1")
schema_2_list = get_bq_table_list("schema_2")
schema_3_list = get_bq_table_list("schema_3")
for table_name, db_type, db, db_schema, process_type, gcp_schema, search_key in db_list:
    # 判斷Bigquery內是否存在table
    if table_name.lower() not in schema_1_list and table_name.lower() not in schema_2_list and table_name.lower() not in schema_3_list:
        exception_text += f"""{table_name.lower()}\n"""
    else:
        if process_type == 'mirror':  # 依資料處理方式分類
            source_count, target_count = check_mirror(table=table_name, source_db_type=db_type, source_db=db,
                                                      source_db_schema=db_schema, bq_schema=gcp_schema)
            if not target_count == source_count:  # 記錄不符合的上傳資料
                NG_text += f"""DB_type：{db_type}, Table：{table_name.lower()}, Source count：{source_count}, GCP count：{target_count}, method：{process_type}\n """
        elif process_type == 'append':
            source_count, target_count = check_append(table=table_name, source_db_type=db_type, source_db=db,
                                                      source_db_schema=db_schema, bq_schema=gcp_schema,
                                                      search_keyword=search_key)
            if not target_count == source_count:  # 記錄不符合的上傳資料
                NG_text += f"""DB_type：{db_type}, Table：{table_name.lower()}, Source count：{source_count}, GCP count：{target_count}, method：{process_type}\n"""

record_session.commit()
record_session.close()
# 將不符合結果的資料訊息上傳Slack
msg_data = str()
put_headers = {'Content-Type': 'application/json'}
if len(NG_text) > 58 and len(exception_text) > 27:
    msg_data = {
        "text": f"""{NG_text}\n\n{exception_text}"""
    }
elif len(NG_text) > 58 and len(exception_text) <= 27:
    msg_data = {
        "text": f"""{NG_text}"""
    }
elif len(NG_text) <= 58 and len(exception_text) <= 27:
    msg_data = {"text": f"""{NG_text}資料來源與Bigquery資料吻合"""}
html = requests.post('https://hooks.slack.com/services/tocken', headers=put_headers, json=msg_data)
