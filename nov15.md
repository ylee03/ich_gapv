# 연속형 자료 다루기
- 2021년 11월 15일

## 자료 소개

mimic-4의 chartevents로부터 혈압만 골라낸다.  첫 7행만 표시되었지만 실제로 이 쿼리는 6,768,759행을 생산할 것이다. 이 쿼리와 쿼리의 결과는 bigquery 안의 별개의 테이블로 저장해 두는 것이 좋다(다음 소단원부터는 three_bps라는 이름으로 이 테이블이 저장되어 있다고 가정하고 진행할 것이다). 

```{sql connection = con, max.print = 7}
WITH t1 AS (
select *
from physionet-data.mimic_icu.chartevents
where itemid IN (
    220052, 225312, 220181 -- mean BP
    , 220050, 225309, 220179 -- systolic BP
    , 220051, 225310, 220180 -- diastolic BP
)
)
, t2 AS (
    select  subject_id, hadm_id, stay_id, charttime
    from t1
    group by subject_id, hadm_id, stay_id, charttime
)
, t3 as (
select t2.*
    , case when t1.itemid IN (220052, 225312, 220181) then valuenum else null end as mbp
    , case when t1.itemid IN (220050, 225309, 220179) then valuenum else null end as sbp
    , case when t1.itemid IN (220051, 225310, 220180) then valuenum else null end as dbp
from t2
join t1 
on t2.stay_id = t1.stay_id
    AND t2.charttime = t1.charttime
)
, t4 as (
select t3.* except (mbp, sbp, dbp)
    , max(mbp) over (partition by stay_id, charttime) mean_bp
    , max(sbp) over (partition by stay_id, charttime) systolic_bp
    , max(dbp) over (partition by stay_id, charttime) diastolic_bp
    , row_number() over (partition by stay_id, charttime) seq_num 
from t3
)

select extract(date from charttime) chartdate,
    * except (seq_num)
from t4
where seq_num = 1
order by stay_id, charttime
```


## BQ에서

빅쿼리에서 gAPV를 계산하는 절차를 만들어 두었다. 이 쿼리가 작동하기 위해서는 three_bps가 존재해야 하며 쿼리 실행 후에는 mbps라는 이름의 테이블이 생길 것이다. 동일한 절차를 적용해서 sbps와 dbps도 만들 수 있을 것이다.

```{sql connection = con}
create or replace table mimickers.voila.mbps AS
with 
t0 AS (
	select *
	from mimickers.voila.three_bps
	where (systolic_bp < 300 AND systolic_bp > 20)
		AND (diastolic_bp < 225 AND diastolic_bp > 5)
		AND (diastolic_bp + 5 < systolic_bp)
)

, t1 AS (
	select subject_id, hadm_id, stay_id, chartdate, charttime
		, first_value(charttime)
			over (partition by stay_id, chartdate order by charttime) startcharttime	
        , lag(charttime) over (partition by stay_id, chartdate order by charttime) prevcharttime
		, lead(charttime)
			over (partition by stay_id, chartdate order by charttime) nexcharttime
        , mean_bp bp
	    , first_value(mean_bp) 
			over (partition by stay_id, chartdate order by charttime) basebp
	    , lag(mean_bp) 
			over (partition by stay_id, chartdate order by charttime) prevbp  
	    , lead(mean_bp) 
			over (partition by stay_id, chartdate order by charttime) nextbp
        , case when mean_bp between 71 AND 80 then 1 else 0 end as below80
        , case when mean_bp between 61 AND 70 then 1 else 0 end as below70
        , case when mean_bp between 56 AND 60 then 1 else 0 end as below60
        , case when mean_bp between 51 AND 55 then 1 else 0 end as below55
        , case when mean_bp <= 50 then 1 else 0 end as below50
	from t0
)
, t2 AS (
select *
    , lag(below80) over (partition by stay_id, chartdate order by charttime) * below80 consecutive_below80
    , lag(below70) over (partition by stay_id, chartdate order by charttime) * below70 consecutive_below70
    , lag(below60) over (partition by stay_id, chartdate order by charttime) * below60 consecutive_below60
    , lag(below55) over (partition by stay_id, chartdate order by charttime) * below55 consecutive_below55
    , lag(below50) over (partition by stay_id, chartdate order by charttime) * below50 consecutive_below50
    , abs(nextbp - bp) diffbp
    , sum(abs(nextbp - bp)) 
		over (partition by stay_id, chartdate order by charttime 
			ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sum_diffbp
    , datetime_diff(charttime, prevcharttime, minute) tk
    , datetime_diff(charttime, startcharttime, minute) tk0
from t1
    )

select *
	, stddev(bp) over (partition by stay_id, chartdate order by charttime
        rows between unbounded preceding and unbounded following) sd_map
    , sum(consecutive_below80) over (partition by stay_id, chartdate order by charttime
        rows between unbounded preceding and unbounded following) sum_con_80
    , sum(consecutive_below70) over (partition by stay_id, chartdate order by charttime
        rows between unbounded preceding and unbounded following) sum_con_70
    , sum(consecutive_below60) over (partition by stay_id, chartdate order by charttime
        rows between unbounded preceding and unbounded following) sum_con_60
    , sum(consecutive_below55) over (partition by stay_id, chartdate order by charttime
        rows between unbounded preceding and unbounded following) sum_con_55
    , sum(consecutive_below50) over (partition by stay_id, chartdate order by charttime
        rows between unbounded preceding and unbounded following) sum_con_50                
    , case when tk0 = 0 then NULL else sum_diffbp/tk0 end AS gAPV
from t2
```

테이블의 필드는 직관적으로 읽을 수 있지만 그래도 표시해 두었다.

* subject_id
* hadm_id
* stay_id
* chartdate
* charttime
* startcharttime
* prevcharttime
* nextcharttime
* bp
* basebp
* prevbp
* nextbp
* below80
* below70
* below60
* below55
* below50
* consecutive_below80
* consecutive_below70
* consecutive_below60
* consecutive_below55
* consecutive_below50
* diff_bp
* sum_diffbp
* tk
* tk0
* sd_map
* sum_con_80
* sum_con_70
* sum_con_60
* sum_con_55
* sum_con_50
* gAPV

## R에서

R에서 읽을 수 있는 작은 형태의 혈압 자료를 미리 만들어 csv로 저장해 두었으니 읽으면서 시작한다. 597명이고 55,017행이 나오면 맞다. 코딩 연습에 좋다.

```{r prompt = T, echo = T}
r_bps <- read.csv("three_bps.csv", header = TRUE, stringsAsFactors = FALSE) #
length( unique(r_bps$hadm_id) )
str(r_bps)
```

### 개별 환자(hadm_id)들의 혈압을 날짜별로 요약(평균, 중위수, 표준편차, 최대/최소값)하기 위해서 어떻게 접근해야 할까?

환자별로 mean_bp의 평균을 계산하려면 이렇게 하고

```{r prompt = T, echo = T, max.print = 9}
tapply(r_bps$mean_bp, r_bps$hadm_id, function(x) mean(x, na.rm = TRUE)) #
```

환자별, 날짜별로 평균을 계산하려면 이렇게 할 수 있지만 약간 문제가 있다. 두 개의 군집변수가 놓일 땐 `list()` 함수로 묶되, `charttime` 필드에는 시간까지 들어 있으므로 `as.Date()` 함수로 날짜만 떼어낸 새 필드 `chartdate`를 이용한다.

```{r prompt = T, echo = T}
tapply(r_bps$mean_bp, 
	list(r_bps$hadm_id, r_bps$chartdate), 
		function(x) mean(x, na.rm = TRUE) ) #
```

눈이 보이는 모든 값이 NA로 나올 것이다. 다음처럼 20002454 환자만 골라서 출력하면 제대로 결과가 보일 것이다.

```{r prompt = T, echo = T}
with( subset(r_bps, hadm_id = 20002454),
	tapply(mean_bp, chartdate, function(x) mean(x, na.rm = T)) )
```

완전히 같은 방법으로 함수 `median(), max(), min()`을 대입하면 환자별 일자별 요약값을 얻을 수 있다.

### Generalized Arterial Pressure Variability 계산

gAPV를 환자별, 날짜별로 계산하는 것이 목표이다. Mascha의 2015년 Anesthesiology 논문을 기반으로 식을 구성하면 이렇다.

\\[
gARV = \frac{1}{T}{
	\sum_{k=1}^{N-1}{|BP_{k+1}-BP_{k}|}
}
\\]

복잡해 보이지만 매 행(k)에서 다음 행(k+1)의 BP와의 차이에 절대값을 취한 것을 모두 더한 뒤에 총시간 T로 나눈 값이 gARV이다. 20002454 환자를 예시하여 보면 3월 2일에 13회 BP를 측정하였고 3월 3일에는 8회, 8월 30일에는 5회 등등이다.

```{r prompt = T, echo = T}
subset(r_bps, hadm_id = 20002454, select = c("charttime", "mean_bp"))[1:30,]
```

소규모 테이블 r_bps를 가지고 R에서 BQ와 같은 테이블(mimickers.voila.mbps)을 만들어 보라.

