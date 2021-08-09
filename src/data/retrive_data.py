from pyaisdb.database import DB

db = DB()
for i in range(30):
    sql = f"""with blue_data as (SELECT time_info, lag(time_info, 1) OVER (ORDER by time_info ASC) as next_time, sog, 
  ST_Distance(pos::geography, lag(pos::geography, 1) OVER (ORDER by time_info ASC)) as dist
	FROM projects._49145341_d2e2f_blue_data_varmdo
	where time_info < '2020-07-19 23:59:59+02'
                  and time_info > '2020-07-10 00:00:00+02'
)
select sum(dist)/1852
from blue_data
where sog >= {i} and sog < {i + 1}"""
    # print(sql)
    #print(f'{i + 0.5} {round(db.execute_and_return(sql)[0][0], 2)}')

    sql = f"""select sum(st_length(segment::geography)) / 1852
from segments_sjfv_2020
where sog>={i} and sog < {i+1}
and mmsi=265520390
and date2 < '2020-07-19 23:59:59+02'
and date1 > '2020-07-10 00:00:00+02' """
    print(f'{i + 0.5} {round(db.execute_and_return(sql)[0][0], 2)}')