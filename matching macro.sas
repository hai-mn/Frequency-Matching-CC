* Starting on 8/18/2019;
%include "D:\data\MCBCR\BCSC mapping programs\2019 (Garth working on this folder)\(0) starter 2019-02.sas";
;

libname mcbcr "D:\data\MCBCR\Hai\SAS Datasets\Matching cc";
ods html; ods listing;
quit;
%m(cc.cancers_for_matching_final2);
%m(pa.patient_with_examid);
proc print data= pa.patient_with_examid (firstobs=1000000 obs=1000033); 
	var fexam_date fexam_year first_control_fexam_year; 
	by studyidn; 
	id studyidn; 
run;

/* 
- Sample 2 controls: 1 case, 
- Frequency matched on age at first exam and facility of first exam and year of first exam;
*/

data case_dataset;
	set cc.cancers_for_matching_final2;
run;* 10055  window, decade, caseid;
%m(case_dataset);

* Get rid of cases in control dataset;
data control_dataset;
	merge 
			pa.patient_with_examid
			cc.cancers_for_matching_final2 (in=c keep=caseid rename=(caseid=studyidn));
	by studyidn;
	if c=0;
run;
/*proc sql;
	create table control_dataset2 as
		select * from pa.patient_with_examid
			where studyidn not in (select caseid from cc.cancers_for_matching_final2);
quit; *  2,412,179 obs and 35 vbls;*/

%m(control_dataset); *2,412,179 window, decade, studyidn;
%m(case_dataset);  

* II- Exact match on decade, firstyear and window, with 2 fixed number of controls;
proc sql;
create table controls_ID
	as select
		one.case_age   as case_age,
		two.age        as control_age,
		one.caseid     as case_id,
		two.studyidn   as control_id,
		one.decade     as case_decade,
		two.decade     as control_decade,
		one.window     as case_window,
		two.window     as control_window,
		one.first_case_fexam_year    as case_year,
		two.first_control_fexam_year as control_year,
		1 as caco
	from case_dataset one, control_dataset two
	where (one.case_age=two.age    and
		   one.window  =two.window and
		   one.first_case_fexam_year=two.first_control_fexam_year);
quit; * 2,520,716;
%m(controls_ID);  

proc sort data=controls_ID;
	by case_id;
run; * 2,520,716;
;



%m(controls_ID);
;
%macro create_final(iter);
%do match_num = 1 %to &iter.;

	* when match_num=1 we create a copy of the dataset;
	data controls_ID_&match_num;
	set  controls_ID;
	if caco = &match_num;
	run;

*create macro variable; /* The %SYSEVALF function performs floating-point arithmetic and returns a value that is formatted using the BEST32. format. The result of the evaluation is always text.*/ 
%let num = %SYSEVALF(&match_num-1);

	data controls_ID_&match_num;
	set cc_ID_&num; 					/* does not yet exist and will create error first time but that's ok */
	if caco ^= &match_num; 				*no one on this dataset first time around;
run;

data controls_ID2 not_enough;  *create 2 datasets;
	set controls_ID_&match_num;
	by case_id;
	retain ratio;
	if first.case_id then ratio = 1;
	if ratio le 2 then do;
		output controls_ID2;
		ratio = ratio + 1;
		end;
	if last.case_id then do;
		if ratio le 2 then output not_enough;
		end;
run;
* when match_num=1 we matched 31,488 controls, and all but 142 ;
* there are duplicates in here we have to deal with later;


data sample;
	merge controls_ID2
			not_enough(in=e);
	by case_id;
	if e then delete;
run;


*remove duplicate controls;
proc sort data=sample nodupkey;
	by control_id;
run;

* how about the case just match with only 1 control ==> need to exclude!!! and keep duplicate;
proc sort data=sample;
	by case_id;
run;
data single dup;     
	set sample;     
	by case_id;     
		if first.case_id and last.case_id          
			then output single;     
			else output dup;
run;

* Stack up to the matching control dataset;
proc append base=mcbcr.final_dataset_&iter. data=dup; 
run;

* eliminate the study of case and control from the pool;
proc sql;
	create table cc_ID_&match_num
		as select * from controls_ID_&match_num
			where case_ID not in (select case_ID from dup) and
					control_ID not in (select control_ID from dup);
quit;

%end;
%mend;

%create_final(16);
%create_final(17);
%create_final(20);

;;
*%m(mcbcr.final_dataset);
/*
proc sort data=mcbcr.final_dataset2; by case_id; run;
proc print data=mcbcr.final_dataset2 (obs=100);
	by case_id;
	id case_id;
run;

proc sort data=sample; by case_id; run;
proc print data=sample (obs=100);
	by case_id;
	id case_id;
run;

data single dup;     
	set sample;     
	by case_id;     
		if first.case_id and last.case_id          
			then output single;     
			else output dup;
run;
proc print data=dup (obs=100);
	by case_id;
	id case_id;
run;
proc print data=single (obs=100);
	by case_id;
	id case_id;
run;

proc print data=mcbcr.final_dataset (obs=1000);
	by case_id;
	id case_id;
run;
*/
/* Case:Control = 1:2
1- : 3808 ==> Total  controls ==> matched  cases
2- : 2948
3- : 2096
4- : 1448
5- : 932
...
10- : 86 ==> 12,720 controls
...
12- : 34 ==> 12,812
....
14- : 16 ==> 12,850

....
16- : 2 ==> 12,856
17- : 0
*/
************************************************************************************************************************; 
************************************************************************************************************************; 

*%m(cc_ID_1);
%m(mcbcr.final_dataset_17);

proc contents  data=mcbcr.final_dataset_17; run;
proc freq  data=mcbcr.final_dataset_17; tables caco ratio; run;

data cases;
set  mcbcr.final_dataset_17;
rename case_age=age case_site=site case_id=studyid case_year=year;
keep case:;
run;
proc sort nodupkey data=cases; by studyid; run;
proc contents; run; *6,428;

data controls;
set  mcbcr.final_dataset_17;
rename control_age=age control_site=site control_id=studyid control_year=year;
keep control:;
proc sort nodupkey data=controls; by studyid; run;
proc contents; run; *12,856;


data cacostudy;
set 
cases (in=case)
controls (in=control)
; cc=case;
run;
proc contents; run; *19,284;

