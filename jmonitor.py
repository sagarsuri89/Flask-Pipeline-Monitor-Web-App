from flask import Flask,render_template
import teradata
import os

os.environ['ODBCINI'] = "/home/mnatarajan/.odbc.ini"
 
udaExec = teradata.UdaExec (appName="IMA Job Monitor", version="1.0",
        logConsole=False)
 
session = udaExec.connect(method="odbc", system="tdwd.snc1", DSN="test_dsn",
        username="mnatarajan", password="XXXXXXX");
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
          from sandbox.ima_intl_jb_mon where rundt=current_date)a\
          left join\
          (select ab as job_name,avg((case when status=2 then current_timestamp when status=3 then end_date end -start_date) hour(2) to MINUTE) as avg_duration,\
          case when trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) as int)/60)<10 then '0'||cast(trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) as int)/60) as varchar(5)) else cast(trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) as int)/60) as varchar(5)) end ||':'||\
          case when trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) mod 60 as int))<10 then '0'||cast(trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) mod 60 as int))as varchar(5)) else cast(trim(cast(avg(extract(hour from start_date)*60+extract(minute from start_date)) mod 60 as int))as varchar(5)) end avg_starttime\
          from sandbox.ima_intl_jb_mon \
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
          from sandbox.ima_intl_jb_mon where rundt=current_date""")
      rows1=row1.fetchall()
      print("totfetch completed")     
      for row1 in rows1:     
          print(row1.completion_status)
      return render_template('jmon_temp.html',rows=rows,rows1=rows1)

@app.route('/transmetric')
def tableread1():
      row=session.execute("""SELECT transaction_date,metric,cast(variancefromprevday as varchar(8))||'%' as variance FROM sandbox.ima_dq_alert where transaction_date=CURRENT_DATE-1""")
      rows=row.fetchall()    
      for row in rows:     
          print("1")
      return render_template('trans_metric.html',rows=rows)

@app.route('/ifgtorders')
def tableread2():
      row=session.execute("""select transaction_date, count(distinct order_uuid) from sandbox.ima_intl_fgt_v2 \
        where transaction_date>current_date-7 group by transaction_date order by transaction_date""")
      rows=row.fetchall()   
      for row in rows:     
          print(row.transaction_date)
      return render_template('IFGT_Orders.html',rows=rows)

@app.route('/promos')
def tableread3():
      row=session.execute("""select transaction_date, count(distinct order_uuid) from sandbox.ima_intl_promo_txns_v2 \
        where transaction_date>current_date-7 group by transaction_date order by transaction_date""")
      rows=row.fetchall()  
      for row in rows:     
          print(row.transaction_date)
      return render_template('Promos_count.html',rows=rows)

@app.route('/attrcount')
def tableread4():
      row=session.execute("""select ifgt.transaction_date, ifgt.traffic_source,count(*) FROM sandbox.ima_intl_fgt_v2 ifgt \
        group by ifgt.transaction_date, ifgt.traffic_source where ifgt.transaction_date > current_date-3 order by traffic_source,transaction_date""")
      rows=row.fetchall()  
      for row in rows:     
          print(row.transaction_date)
      return render_template('Attr_count.html',rows=rows)

@app.route('/GAdds')
def tableread5():
      row=session.execute("""select transaction_date,\
          activations,\
          reactivations,\
          deactivations,\
          active_customers,\
          cast (activations+reactivations-deactivations  as VARCHAR(100)) adds_act_logic,\
          cast ( active_customers-lag_cust as VARCHAR(100)) adds_active_logic,\
          cast (abs(activations+reactivations-deactivations)- abs(active_customers-lag_cust) as VARCHAR(100)) diff_adds\
          from (\
          SELECT \
          a.transaction_date as transaction_date,\
          SUM(activations) AS activations,\
          SUM(reactivations) AS reactivations,\
          SUM(deactivations) AS deactivations,\
          SUM(active_customers) AS active_customers,\
          lag(SUM(active_customers)) over(order by transaction_date) lag_cust\
          FROM\
          (\
          select \
          transaction_date,\
          count(distinct case when action = 'authorize' and is_activation = 1 then parent_order_uuid end) as activations,\
          count(distinct case when action = 'authorize' and is_reactivation = 1 then parent_order_uuid end) as reactivations,\
          SUM(0) AS deactivations,\
          SUM(0) AS active_customers\
          from sandbox.ima_intl_fgt_v2\
          where action = 'authorize' and is_attribution_active=1\
          and transaction_date BETWEEN current_date-7 AND current_date-1\
          group by 1\
          UNION\
          select \
          deactivation_date AS transaction_date,\
          SUM(0) as activations,\
          SUM(0) as reactivations,\
          count(distinct case when action = 'authorize' and is_deactivation = 1 then parent_order_uuid end) as deactivations,\
          SUM(0) AS active_customers\
          from sandbox.ima_intl_fgt_v2\
          where action = 'authorize' and is_attribution_active=1\
          and deactivation_date BETWEEN current_date-7 AND current_date-1\
          group by 1\
          UNION\
          SELECT\
          CAST(ddy.day_rw AS DATE) AS transaction_date,\
          SUM(0) as activations,\
          SUM(0) as reactivations,\
                    SUM(0) AS deactivations,\
          COUNT(DISTINCT ifgt.unified_user_id) AS active_customers\
          FROM sandbox.ima_intl_fgt_v2 AS ifgt\
          INNER JOIN user_groupondw.dim_day AS ddy ON ifgt.transaction_date BETWEEN (ddy.day_rw - INTERVAL '364' DAY) AND ddy.day_rw\
          INNER JOIN user_groupondw.gbl_dim_country AS cn ON cn.country_key = ifgt.country_id\
          WHERE ddy.day_rw BETWEEN current_date-8 AND current_date-1\
          AND ifgt.action = 'authorize' and is_attribution_active=1\
          and is_order_canceled=0\
          AND ifgt.txn_amount_loc > 0\
          group by 1\
          ) a\
          GROUP BY 1\
          ) a where transaction_date between  current_date-7 AND current_date-1""")
      rows=row.fetchall()    
      for row in rows:     
          print(row.activations)
      return render_template('Gross_adds.html',rows=rows)

@app.route('/GP')
def tableread6():
      row=session.execute("""select metric_date,sum(gp) as GP,count(*) as Cnt from sandbox.ima_intl_performance_metrics_v2 where metric_date>current_date-10 group by metric_date order by 1,2""")
      rows=row.fetchall()  
      for row in rows:     
          print(row.GP)
      return render_template('GP_wbr.html',rows=rows)

@app.route('/Traffic')
def tableread7():
      row=session.execute("""select a1.report_date,a1.traffic_source,a1.sub_platform,'UDVV' as metrics,cast(a1.cnt_t as int) current_val,cast(a2.cnt_t as int) last_val,cast(cast((NullIf((a1.cnt_t - a2.cnt_t),0)/NullIf(a1.cnt_t,0))*100 as int) as varchar(15))||'%' diff_perc\
          from \
          (\
          select \
          report_date,\
          traffic_source,\
          case when sub_platform in ('iphone','ipad') then 'IOS' \
          when sub_platform in ('android','touch','orderup-touch') then 'Mobile' \
          when sub_platform in ('web','orderup-web') then 'Desktop'\
          else sub_platform\
          end as sub_platform,\
          cast(sum(uniq_deal_view_visitors) as decimal(18,2)) cnt_t\
          from user_edwprod.agg_gbl_traffic\
          where report_date > current_date - 4 \
          group by 1,2,3\
          ) A1\
          join \
          (\
          select \
          report_date,\
          traffic_source,\
          case when sub_platform in ('iphone','ipad') then 'IOS' \
          when sub_platform in ('android','touch','orderup-touch') then 'Mobile' \
          when sub_platform in ('web','orderup-web') then 'Desktop'\
          else sub_platform\
          end as sub_platform,\
          cast(sum(uniq_deal_view_visitors) as decimal(18,2)) cnt_t\
          from user_edwprod.agg_gbl_traffic\
          where report_date > current_date - 4\
          group by 1,2,3\
          ) A2\
          on a1.traffic_source = a2.traffic_source\
          and a1.sub_platform = a2.sub_platform\
          and a2.report_date = a1.report_date-1\
          where a1.report_date = current_date - 1\
          union all\
          select a1.report_date,a1.traffic_source,a1.sub_platform,'UV',cast(a1.cnt_t as int)current_val,cast(a2.cnt_t as int) last_val,cast(cast((NullIf((a1.cnt_t - a2.cnt_t),0)/NullIf(a1.cnt_t,0))*100 as int) as varchar(15))||'%' diff_perc\
          from \
          (\
          select \
          report_date,\
          traffic_source,\
          case when sub_platform in ('iphone','ipad') then 'IOS' \
          when sub_platform in ('android','touch','orderup-touch') then 'Mobile' \
          when sub_platform in ('web','orderup-web') then 'Desktop'\
          else sub_platform\
          end as sub_platform,\
          cast(sum(uniq_visitors) as decimal(18,2)) cnt_t\
          from user_edwprod.agg_gbl_traffic\
          where report_date > current_date - 4 \
          group by 1,2,3\
          ) A1\
          join \
          (\
          select \
          report_date,\
          traffic_source,\
          case when sub_platform in ('iphone','ipad') then 'IOS' \
          when sub_platform in ('android','touch','orderup-touch') then 'Mobile' \
          when sub_platform in ('web','orderup-web') then 'Desktop'\
          else sub_platform\
          end as sub_platform,\
          cast(sum(uniq_visitors) as decimal(18,2)) cnt_t\
          from user_edwprod.agg_gbl_traffic\
          where report_date > current_date - 4 \
          group by 1,2,3\
          ) A2\
          on a1.traffic_source = a2.traffic_source\
          and a1.sub_platform = a2.sub_platform\
          and a2.report_date = a1.report_date-1\
          where a1.report_date = current_date - 1\
          union all\
          select a1.report_date,a1.traffic_source,a1.sub_platform,'UDV',cast(a1.cnt_t as int) current_val,cast(a2.cnt_t as int) last_val,cast(cast((NullIf((a1.cnt_t - a2.cnt_t),0)/NullIf(a1.cnt_t,0))*100 as int) as varchar(15))||'%' diff_perc \
          from \
          (\
          select \
          report_date,\
          traffic_source,\
          case when sub_platform in ('iphone','ipad') then 'IOS' \
          when sub_platform in ('android','touch','orderup-touch') then 'Mobile' \
          when sub_platform in ('web','orderup-web') then 'Desktop'\
          else sub_platform\
          end as sub_platform,\
          cast(sum(uniq_deal_views) as decimal(18,2)) cnt_t\
          from user_edwprod.agg_gbl_traffic\
          where report_date > current_date - 4\
          group by 1,2,3\
          ) A1\
          join \
          (\
          select \
          report_date,\
          traffic_source,\
          case when sub_platform in ('iphone','ipad') then 'IOS' \
          when sub_platform in ('android','touch','orderup-touch') then 'Mobile' \
          when sub_platform in ('web','orderup-web') then 'Desktop'\
          else sub_platform\
          end as sub_platform,\
          cast(sum(uniq_deal_views) as decimal(18,2)) cnt_t\
          from user_edwprod.agg_gbl_traffic\
          where report_date > current_date - 4\
          group by 1,2,3\
          ) A2\
          on a1.traffic_source = a2.traffic_source\
          and a1.sub_platform = a2.sub_platform\
          and a2.report_date = a1.report_date-1\
          where a1.report_date = current_date - 1\
          order by 1,2,3""")
      rows=row.fetchall()  
      for row in rows:     
          print(row.traffic_source)
      return render_template('Traffic_metrics.html',rows=rows)

@app.route('/jobhist/<jobname>')
def tableread8(jobname):
      print(jobname)
      row=session.execute("""select rundt as run_date,ab as job_name,start_date,end_date,\
        (end_date-start_date) hour(1) to MINUTE as duration from sandbox.ima_intl_jb_mon \
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


