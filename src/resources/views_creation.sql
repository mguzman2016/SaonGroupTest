create or replace view vw_report_1
AS
select              dc.company
				   ,count(distinct fa.applicantid ) as "Applications"
				   ,count(distinct da.advertid)  	as "Jobs"
from                ft_applicants 			fa
				   ,dt_company 				dc
				   ,dt_advert 				da
where 				fa.companyid = dc.companyid
and 				fa.advertid  = da.advertid
group by 			dc.company
;

create or replace view vw_report_2
as
select 				cast(dd."year" as varchar(50))  	as  "year"
				   ,case
				      when len(cast(dd."month" as varchar(50))) = 1 then
				      	 concat('0',cast(dd."month" as varchar(50)))
				      else
				      		cast(dd."month" as varchar(50))
				    end as "month"
				   ,case
				      when len(cast(dd."day" as varchar(50))) = 1 then
				      	 concat('0',cast(dd."day" as varchar(50)))
				      else
				      		cast(dd."day" as varchar(50))
				    end as "day"
				   ,count(distinct fa.applicantid) as "Applications"
from 				ft_applicants 			fa
				   ,dt_date 				dd
where 				fa.dateid 			  = dd.dateid
group by 			dd."year"
				   ,dd."month"
				   ,dd."day"
;

create or replace view vw_report_3
as
select 				dc.sector
				   ,count(distinct applicantid) as "applications"
from 				dt_company 				dc
				   ,ft_applicants 			fa
where 				dc.companyid 		  = fa.companyid
group by 			dc.sector
;


create or replace view vw_report_4
as
select				case
					when (fa.age/10 = 1 ) then
						cast((fa.age/10) as varchar(10)) ||'8'||' '||'-'||' '||cast((fa.age/10) as varchar(10))||'9'
					else
						cast((fa.age/10)*10 as varchar(10)) ||' '||'-'||' '||cast((fa.age/10) as varchar(10))||'9'
				    end as "age_range"
				   ,count(distinct fa.applicantid) as "applications"
from 				ft_applicants 		fa
where 				fa.age > 0
group by 			(fa.age/10)
;

create or replace view vw_report_5
as
select   			da.title  as "job"
				   ,count(distinct fa.applicantid) as "applications"
from 				dt_advert 		da
				   ,ft_applicants 	fa
where 				da.advertid = fa.advertid
group by 			da.title
;

create or replace view vw_report_6
as
select  		    dc.company
				   ,cast(dda."year" as varchar(50))  	as  "year"
				   ,case
				      when len(cast(dda."month" as varchar(50))) = 1 then
				      	 concat('0',cast(dda."month" as varchar(50)))
				      else
				      		cast(dda."month" as varchar(50))
				    end as "month"
				   ,case
				      when len(cast(dda."day" as varchar(50))) = 1 then
				      	 concat('0',cast(dda."day" as varchar(50)))
				      else
				      		cast(dda."day" as varchar(50))
				    end as "day"
				   ,count(distinct fa.advertid) as "jobs_posted"
from 				dt_advert 			da
				   ,dt_company 			dc
				   ,dt_date_advert 		dda
				   ,ft_applicants 		fa
where 				da.advertid 	= fa.advertid
and 				dc.companyid  	= fa.companyid
and 				da.dateadvertid = dda.dateadvertid
group by 			dc.company
				   ,dda."day"
				   ,dda."month"
				   ,dda."year"
;