from pyaisdb.database import DB
from tqdm import tqdm

db = DB()

sql ="""--drop table projects._49145341_d2e2f_blue_data_varmdo;
create table projects._49145341_d2e2f_blue_data_varmdo (row_id serial, time_info timestamp with time zone, sog float, pos geometry(point, 4326))"""
db.execute(sql)

with open('..\\varmdo-gps-july-2020.csv') as f:
    data = f.readlines()[1:]
    for row in tqdm(data):
        row = row.split(',')
        sql = f"insert into projects._49145341_d2e2f_blue_data_varmdo (time_info, sog, pos) VALUES ('{row[0]}', {row[1]}, st_geomfromtext('Point({row[3]} {row[2]})', 4326))"
        a = db.execute(sql, commit=False, return_error=True)
    db.commit()