##########################################################################################################################
################################################## RTDS ######################################################
##########################################################################################################################



# Setup ----
library(data.table)
library(dplyr)
library(stringr)


setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
# make sure the oracle_connection.R is in the same folder as your script or put in full path into the source function
source("oracle_connection.R") #need packages getPass, svDialogs, RJDBC, DBI


# connect to CAS ----


CAS <- createConnection(sid = "", username = "")


rtdsQuery <- "

--tmp: Extract the radiotherapy episode data where the radiotherapy start date is in year 2020
with tmp as
    (select distinct patientid, tumourid
    , radiotherapyepisodeid
    , radiotherapydiagnosisicd
    , radiotherapyintent
    , min(treatmentstartdate) over (partition by radiotherapyepisodeid, orgcodeprovider) as treatmentstartdate
    , decode(ORGCODEPROVIDER,'RH1','R1H','RNL','RNN','RGQ','RDE','RQ8','RAJ','RDD','RAJ','RBA','RH5','RA3','RA7','RDZ','R0D','RD3','R0D','RNJ','R1H','RXH','RYR','E0A','RYR', 'RM3', 'RBV', ORGCODEPROVIDER) as orgcodeprovider 
	, first_value(radiotherapydiagnosisicd) IGNORE NULLS over(partition by orgcodeprovider, radiotherapyepisodeid order by apptdate asc) as first_icd10
    from e  --radiotherapy episodes table
    where orgcodeprovider not in ('7A3','7A1','RQF','RP6') --exclude Welsh providers & Moorfields
       and treatmentstartdate between to_date('01/01/2020','dd/mm/yyyy') and to_date('31/12/2020','dd/mm/yyyy')
    )
--tmp1: Apply a site restriction to 'tmp' to get only the radiotherapy episode data for head and neck patients where the radiotherapy start date is in year 2020
, tmp1 as 
    (select *
	 from tmp
	 where ((substr(first_icd10,1,3) BETWEEN 'C00' AND 'C14') or (substr(first_icd10,1,3) BETWEEN 'C30' AND 'C32')) --ICD10 codes for head and neck cancers
    )  
--tmp2: Extract the tumour and patient level data for head and neck patients diagnosed in years 1995 to 2020 
, tmp2 as
   (select distinct ap.patientid, at.tumourid, at.diagnosisdatebest, SUBSTR(at.site_coded,1,3) site_coded
   , SUBSTR(EXTRACT (YEAR FROM ADD_MONTHS (at.DIAGNOSISDATEBEST, -3)),3,2) || SUBSTR(EXTRACT (YEAR FROM ADD_MONTHS (at.DIAGNOSISDATEBEST, 9)),3,2) DIAGNOSISFINYEAR
     from at --tumour table
     inner join ap on at.patientid = ap.patientid --patient table
     where at.gender in ('1','2')  
        and statusofregistration = 'F' 
        and dedup_flag = 1
        and at.DIAGNOSISDATEBEST between to_date('01/01/1995','dd/mm/yyyy') and to_date('31/12/2020','dd/mm/yyyy')
        and ((substr(site_coded,1,3) BETWEEN 'C00' AND 'C14') or (substr(site_coded,1,3) BETWEEN 'C30' AND 'C32'))
   )
--tmp3: Apply a restriction to 'tmp2' to get the tumour and patient level data for head and neck patients diagnosed in years 1995 to 2020 that have had major surgery. 
, tmp3 as
    (select /*+ USE_HASH(tmp2 HL) USE_HASH(HL h) USE_HASH(h op) USE_HASH(op s) */ distinct 
          tmp2.PATIENTID
        , tmp2.tumourid 
        , tmp2.diagnosisdatebest
        , tmp2.site_coded
        , tmp2.DIAGNOSISFINYEAR
        , hl.epikeyanon
        , hl.datayear
        , op.opertn
        , op.opdate
        , op.pos
        , RANK() OVER(PARTITION BY tmp2.patientid, tmp2.tumourid, tmp2.diagnosisdatebest, tmp2.site_coded ORDER BY op.opdate DESC) Rank --Rank the data for each patient and tumour by the surgery/operation dates 
     from tmp2   
     inner join HL on tmp2.patientid = HL.patientid and tmp2.DIAGNOSISFINYEAR = HL.datayear --hes linkage table
     inner join h on hl.epikeyanon = h.epikeyanon and HL.datayear = h.datayear --hes admitted patient table
     inner join op on op.epikeyanon = h.epikeyanon and op.datayear = h.datayear --hes operation table
     inner join s on op.opertn = s.surgery_code --Import this table externally (these are only head and neck surgery codes) --Keep only the H&N surgery codes
     where (((op.opdate-tmp2.diagnosisdatebest between -31 and 365) and tmp2.site_coded in ('C01','C09','C10')) --oropharynx
         or ((op.opdate-tmp2.diagnosisdatebest between -31 and 456) and tmp2.site_coded in ('C05','C11','C14','C30','C31')) --other head and neck
         or ((op.opdate-tmp2.diagnosisdatebest between -31 and 183) and tmp2.site_coded in ('C02','C03','C04','C06')) --oral cavity
         or ((op.opdate-tmp2.diagnosisdatebest between -31 and 456) and tmp2.site_coded in ('C32')) --larynx
         or ((op.opdate-tmp2.diagnosisdatebest between -31 and 183) and tmp2.site_coded in ('C07','C08')) --salivary glands
         or ((op.opdate-tmp2.diagnosisdatebest between -31 and 365) and tmp2.site_coded in ('C12','C13'))) --hypopharynx
    )
-- tmp4: Join 'tmp3' to 'tmp1' to get the radiotherapy treatment data at episode level data for head and neck patients diagnosed 1995 to 2020 that have had major surgery and had radiotherapy treatment in year 2020
, tmp4 as
    (select /*+ USE_HASH(tmp3 tmp1) */ distinct    
          tmp1.radiotherapyepisodeid 
        , tmp1.radiotherapydiagnosisicd 
        , tmp1.treatmentstartdate  
        , tmp3.diagnosisdatebest
        , tmp3.opdate 
        , tmp3.opertn
        , tmp1.orgcodeprovider
        , tr.provider_name
        , ROUND(tmp1.treatmentstartdate - tmp3.opdate,2) date_diff, --the wait in days from date of surgery/operation to start of radiotherapy 
     CASE
       WHEN tmp1.treatmentstartdate - tmp3.opdate BETWEEN 0 AND 41 THEN '1' -- 'Within 6 weeks' --The patient has received radiotherapy treatment within 6 weeks of major surgery 
       WHEN tmp1.treatmentstartdate - tmp3.opdate BETWEEN 42 AND 90 THEN '2' -- 'Between 6 weeks and 3 months'
	   WHEN tmp1.treatmentstartdate - tmp3.opdate BETWEEN 91 AND 150 THEN '3' -- 'Between 3 and 5 months'
       ELSE '4'
     END AS SixWeek   
     from tmp3 
       inner join tmp1 on tmp1.patientid = tmp3.patientid  and tmp1.tumourid = tmp3.tumourid 
       left join tr on tmp1.orgcodeprovider = tr.provider_code --trust name/code table --Extract the trust name
     where tmp3.Rank=1 --Take only the first surgery/operation date to get "the wait in days from date of first surgery/operation to start of radiotherapy"
     and tmp1.radiotherapyintent = '02' 
)
--cohort: Apply a restriction to 'tmp4' to get the radiotherapy treatment data at episode level data for head and neck patients diagnosed 1995 to 2020 that have had major surgery within 91 days and had radiotherapy treatment in year 2020
--cohort: This table is used to calculate metrics 5a and 5b. 
--The term 'adjuvant' is assumed to be within 3 months (which means the 'SixWeek' variable takes value '1' or '2')
, cohort as 
    (select *
    from tmp4
    where SixWeek in ('1','2')
    )
--tmp5: Within each trust, calculate a quartile (number from 1 to 4) to each radiotherapy episode ID.  
, tmp5 as 
    (select distinct orgcodeprovider, provider_name, 
            radiotherapyepisodeid,
            date_diff, 
            NTILE(4) OVER (PARTITION BY provider_name ORDER BY date_diff) AS Quartile
    from cohort  
    )
--denominator: Count the number of distinct radiotherapy episode IDs for head and neck patients diagnosed 1995 to 2020 that have had major surgery within 91 days (3 months) and had radiotherapy treatment in year 2020
, denominator as
    (select count(distinct orgcodeprovider||radiotherapyepisodeid) as new_episodes
    , orgcodeprovider
    , provider_name
    from cohort
    group by orgcodeprovider, provider_name
    )  
--numerator: Count the number of distinct radiotherapy episode IDs for head and neck patients diagnosed 1995 to 2020 that have had major surgery within 42 days (6 weeks) and had radiotherapy treatment in year 2020	
, numerator as
    (select count(distinct orgcodeprovider||radiotherapyepisodeid) as new_episodes
    , orgcodeprovider
    , provider_name
    from tmp4
    where SixWeek = '1'
    group by orgcodeprovider, provider_name, SixWeek
    )
--overall1: This is an additional count that is not used to calculate a metric	
--It counts the number of distinct radiotherapy episode IDs for head and neck patients diagnosed 1995 to 2020 that have had major surgery within 151 days (5 months) and had radiotherapy treatment in year 2020
, overall1 as
    (select count(distinct orgcodeprovider||radiotherapyepisodeid) as new_episodes
    , orgcodeprovider
    , provider_name
    from tmp4
    where SixWeek in ('1','2','3')
    group by orgcodeprovider, provider_name
    )
--overall2: This is an additional count that is not used to calculate a metric	
--It counts the number of distinct radiotherapy episode IDs for head and neck patients diagnosed 1995 to 2020 that have had major surgery and had radiotherapy treatment in year 2020
, overall2 as
    (select count(distinct orgcodeprovider||radiotherapyepisodeid) as new_episodes
    , orgcodeprovider
    , provider_name
    from tmp4
    where SixWeek in ('1','2','3','4')
    group by orgcodeprovider, provider_name
    )
--metric_5a: Calculate the percentage of distinct radiotherapy episode IDs for head and neck patients diagnosed 1995 to 2020 that have had major surgery within 42 days (6 weeks) and had radiotherapy treatment in year 2020	
, metric_5a as 
    (select distinct
         ov2.orgcodeprovider                                                       as orgcodeprovider  
       , ov2.provider_name                                                         as provider_name
       , coalesce(n.new_episodes,0)                                                as numerator_5a 
       , coalesce(d.new_episodes,0)                                                as denominator_5a
       , ROUND(coalesce(n.new_episodes,0)/coalesce(d.new_episodes,0)*100,1)        as six_week_percentage_5a 
       , coalesce(ov1.new_episodes,0)                                              as zero_and_onefifty_5a
	   , coalesce(ov2.new_episodes,0)                                              as unrestricted_5a
     from overall2 ov2  
        left join overall1 ov1  on ov1.orgcodeprovider = ov2.orgcodeprovider
        left join denominator d on d.orgcodeprovider = ov1.orgcodeprovider
        left join numerator n   on n.orgcodeprovider = d.orgcodeprovider
     order by ov2.provider_name
    )
--metric_5b: Calculate the median and interquartile range (IQR) wait in days from date of first surgery/operation 
, metric_5b as 
   (SELECT orgcodeprovider, provider_name,
        ROUND(MIN(date_diff),1) Minimum_5b,                                   --For each trust, calculate the smallest date_diff 
        ROUND(MAX(CASE WHEN Quartile = 1 THEN date_diff END),1) Quartile1_5b, --Within each trust, calculate the date_diff that corresponds to quartile 1 (around 25% of the date_diff values are below this value)
        ROUND(MAX(CASE WHEN Quartile = 2 THEN date_diff END),1) Median_5b,    --Within each trust, calculate the median date_diff 
        ROUND(MAX(CASE WHEN Quartile = 3 THEN date_diff END),1) Quartile3_5b, --Within each trust, calculate the date_diff that corresponds to quartile 3 (around 75% of the date_diff values are below this value)
        ROUND(MAX(date_diff),1) Maximum_5b,                                   --For each trust, calculate the largest date_diff for each trust
        ROUND(MAX(CASE WHEN Quartile = 3 THEN date_diff END) - MAX(CASE WHEN Quartile = 1 THEN date_diff END),1) IQR_5b, --Calculate the difference between quartiles 1 and 3
        coalesce(COUNT(DISTINCT radiotherapyepisodeid),0) AS COUNT_ep_5b      --For each trust, count the number of episodes
        from tmp5
    GROUP BY provider_name, orgcodeprovider
    ORDER BY provider_name
   )
select a.orgcodeprovider, a.provider_name, 
         unrestricted_5a, zero_and_onefifty_5a, numerator_5a, denominator_5a, six_week_percentage_5a,
         COUNT_ep_5b, minimum_5b, maximum_5b, median_5b, quartile1_5b, quartile3_5b, IQR_5b
from metric_5a a
left join metric_5b b on a.provider_name = b.provider_name
				        
"

# extract data for particular years.
rtds <- dbGetQueryOracle(CAS, rtdsQuery, rowlimit = NA)


# close and rm connection ----
dbDisconnect(CAS)

rm(CAS)





