--合并数据配置表
create external table if not exists data_merge_config
(
    hive_table_name string comment '数据处理的hive表',
    data_merge_type string comment '数据处理类型,sum,avg',
    data_merge_dimension string comment '该hive表合并涉及的维度列',
    data_merge_measure string comment '该hive合并涉及的度量列',
    data_merge_partition string comment '该hive表的分区字段',
    del_flag int comment '删除标志'
)
row format delimited fields terminated by ','
stored as textfile
location "/project/edu_edcc/xrding/data/data_merge_config";

--txt
edu_edcc.data_merge_test1,sum,school_id@app_id,user_total_num,pdate,0
edu_edcc.data_merge_test1,sum,school_id@app_id,user_valid_num,pdate,0
edu_edcc.data_merge_test1,avg,school_id@app_id,user_active_rate,pdate,0
edu_edcc.data_merge_test2,avg,school_id@app_id,user_active_rate,pdate,0

edu_edcc.merge_sh_zx_ques_natural_gp_test,sum,province_id@city_id@district_id@school_id@school_name@app_id@app_name,ques_total_num,pdate,0
edu_edcc.merge_sh_zx_ques_natural_gp_test,sum,province_id@city_id@district_id@school_id@school_name@app_id@app_name,ques_use_num,pdate,0

--测试语句
create table data_merge_test1
(
    school_id string,
    app_id string,
    user_total_num bigint,
    user_valid_num bigint,
    user_active_rate double
)
partitioned by(pdate string)
stored as parquet;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table data_merge_test1 partition(pdate)
select "4444000020000002112","1001",10,10,0.10,"2018-04-30"
union all
select "2244000014000000019","1001",20,20,0.20,"2018-04-30"
union all
select "4444000020000001425","1001",30,30,0.30,"2018-04-30"
union all
select "4","1001",40,40,0.40,"2018-04-30";

create table data_merge_test2
(
    school_id string,
    app_id string,
    user_active_rate double
)
partitioned by(pdate string)
stored as parquet;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table data_merge_test2 partition(pdate)
select "1","1001",0.10,"2018-06-11"
union all
select "2","1001",0.20,"2018-06-11";