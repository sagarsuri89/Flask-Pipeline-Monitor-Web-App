from flask import Flask,render_template
import teradata
import os

os.environ['ODBCINI'] = XXX
 
udaExec = teradata.UdaExec (appName="Flask Job Monitor", version="1.0",
        logConsole=False)
 
session = udaExec.connect(method="odbc", system="tdwd.snc1", DSN="test_dsn",
        username="XXXXXXX, password="XXXXXXX");
print("conn step passed") 
app = Flask(__name__)
@app.route('/jmonitor')
def tableread():
      print("into method")
      row=session.execute("""select a.job_name,a.status,b.avg_starttime,a. start_date,a.end_date,a.duration,b.avg_duration,\
          case \
          when  status= 'Completed' and extract(hour from avg_duration)*60+extract(minute from avg_duration) = extract(hour from duration)*60+extract(minute from duration) then 'ON PAR'\
          when  status= 'Completed' and extract(hour from duration)*60+extract(minute from duration) < extract(hour from avg_duration)*60+extract(minute from avg_duration) then 'PROMPT'\
          when  status= 'Completed' and extract(hour from duration)*60+extract(minute from duration) > extract(hour from avg_duration)*60+extract(minute from avg_duration)  and \
          extract(hour from duration)*60+extract(minute from duration)<=extract(hour from avg_duration)*60+extract(minute from avg_duration)+30 then 'MERE DELAY'\
          when  status= 'Completed' and extract(hour from duration)*60+extract(minute from duration) > extract(hour from avg_duration)*60+extract(minute from avg_duration)+31 then 'DELAY'\
          end delay_status\
          from\
          (select ab as job_name, case when status=1 then 'Scheduled' when status=2 then 'Running' when status=3 then 'Completed' end as status, start_date,end_date,\
          (case when status=2 then current_timestamp when status=3 then end_date end -start_date) hour(1) to MINUTE as duration \
          from sandbox.test_tablewhere rundt=current_date)a\
          left join\
          (select ab as job_name,avg((case when status=2 then current_timestamp when status=3 then end_date end -start_date) hour(2) to MINUTE) as avg_duration,\
          case when trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) as int)/60)<10 then '0'||cast(trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) as int)/60) as varchar(5)) else cast(trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) as int)/60) as varchar(5)) end ||':'||\
          case when trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) mod 60 as int))<10 then '0'||cast(trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) mod 60 as int))as varchar(5)) else cast(trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) mod 60 as int))as varchar(5)) end avg_starttime\
          from sandbox.test_table\
          where rundt between date-7 and current_date-1 and status=3 and start_date is not null\
          group by 1)b\
          on a.job_name=b.job_name\
          order by a.status desc,a.start_date desc""")
      print("fetch")
      rows=row.fetchall()
      print("fetch completed")     
      for row in rows:     
          print(row.job_name)
      row1=session.execute("""select cast(cast(round((cast(count(case when  status=3 then ab end) as float)/cast(count(*) as float))*100.00,0) as int)as varchar(8))||'%' as completion_status\
          from sandbox.test_tablewhere rundt=current_date""")
      rows1=row1.fetchall()
      print("totfetch completed")     
      for row1 in rows1:     
          print(row1.completion_status)
      return render_template('jmon_temp.html',rows=rows,rows1=rows1)

@app.route('/transmetric')
def tableread1():
      row=session.execute("""SELECT transaction_date,metric,cast(variancefromprevday as varchar(8))||'%' as variance FROM sandbox.dq_alert where transaction_date=CURRENT_DATE-1""")
      rows=row.fetchall()    
      for row in rows:     
          print("1")
      return render_template('trans_metric.html',rows=rows)

@app.route('/ifgtorders')
def tableread2():
      row=session.execute("""select transaction_date, count(distinct order_uuid) from sandbox.transactions \
        where transaction_date>current_date-7 group by transaction_date order by transaction_date""")
      rows=row.fetchall()   
      for row in rows:     
          print(row.transaction_date)
      return render_template('IFGT_Orders.html',rows=rows)

@app.route('/promos')
def tableread3():
      row=session.execute("""select transaction_date, count(distinct order_uuid) from sandbox.promotions \
        where transaction_date>current_date-7 group by transaction_date order by transaction_date""")
      rows=row.fetchall()  
      for row in rows:     
          print(row.transaction_date)
      return render_template('Promos_count.html',rows=rows)

@app.route('/attrcount')
def tableread4():
      row=session.execute("""select ifgt.transaction_date, ifgt.traffic_source,count(*) FROM sandbox.attribution \
        group by ifgt.transaction_date, ifgt.traffic_source where ifgt.transaction_date > current_date-3 order by traffic_source,transaction_date""")
      rows=row.fetchall()  
      for row in rows:     
          print(row.transaction_date)
      return render_template('Attr_count.html',rows=rows)

@app.route('/jobhist/<jobname>')
def tableread8(jobname):
      print(jobname)
      row=session.execute("""select rundt as run_date,ab as job_name,start_date,end_date,\
        (end_date-start_date) hour(1) to MINUTE as duration from sandbox.job_hist\
        where rundt between date-15 and current_date-1 and status=3 and ab='{}'\
        group by 1,2,3,4,5 order by 1 desc,2,3,4,5""".format(jobname))
      rows=row.fetchall()  
      for row in rows:     
          print(row.run_date)
          print(jobname)
      return render_template('Job_hist.html',rows=rows)

if __name__=='__main__':
    print("last step")
    app.run(host='0.0.0.0')


