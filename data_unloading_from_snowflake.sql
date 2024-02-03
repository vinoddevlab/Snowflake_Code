CREATE OR REPLACE PROCEDURE EA_STAGING.ETL.USP_ARCHIVE_DATA("RUNID" VARCHAR(16777216), "P_PROJECT" VARCHAR(120), "P_STAGE_DB" VARCHAR(50), "P_STAGE_SCHEMA" VARCHAR(50), "P_STAGE_TABLE" VARCHAR(50))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'begin
 declare 

  --variables for execution log 
  v_target_rows integer default 0;
  v_rows_unloaded integer default 0;
  v_rows_deleted integer default 0;
  --variables for retention config
  v_data_retention_value integer default 0;
  v_data_retention_date_time_part varchar(50) default ''''; 
  v_data_archival_column string default '''';
  
  --variables for dynamic queries
  v_retention_query string default ''''; 
  v_fully_qualified_name string default :p_stage_db||''.''||:p_stage_schema||''.''||:p_stage_table;

  --variables for custom exceptions
  v_exception_req_val exception (-20002, ''some mandatory config values are null, please update them in control table.''); 
  v_exception exception (-20003, ''other exception,for more details, read etl_run_detail table using the current runid.''); 
  v_exception_row_counts exception (-20004, ''unloaded rows and actual row counts are not matching.'');
  begin
    --read retention config from control table for the given table.
    select data_retention_value,data_retention_date_time_part,data_archival_column into 
    :v_data_retention_value,:v_data_retention_date_time_part,:v_data_archival_column from ea_staging.etl.etl_control_master
    where lower(project)=lower(:p_project) and lower(stage_db)=lower(:p_stage_db) and lower(stage_schema)=lower(:p_stage_schema) and lower(stage_table)=lower(:p_stage_table);

    
    --check if all variables holding values then unload data into external stage else raise an exception.
    if ((:v_data_retention_value||:v_data_retention_date_time_part||:v_data_archival_column) is not null) then

       --unload data into external storage
       execute immediate  ''copy into @STG_AZURE_BUSINESSPERFORMANCE/''||:p_stage_db||''/''||:p_stage_schema||''/''||:p_stage_table||
                              '' from ( select * from ''||:v_fully_qualified_name||'' where ''||:v_data_archival_column||
                              '' < dateadd(''||:v_data_retention_date_time_part||'',-''||v_data_retention_value||'',current_date()))''||
                              '' partition by(concat(date_part(year,''||:v_data_archival_column||''::date)::string,''''-'''',
                                             right(''''00''''||date_part(month,''||:v_data_archival_column||''::date),2)::string,''''-'''',
                                             right(''''00''''||date_part(day,''||:v_data_archival_column||''::date),2)::string)
                                           ) file_format=ODS_LIVE.DBO.FF_PARQUET_ODS_STAGING_SNAPPY
                                            header = true;'';
       
       select "rows_unloaded" into v_rows_unloaded from table(result_scan(last_query_id()));

       --get record count from the table to compare, if match, then proceed delete
       execute immediate ''select count(1) as target_count from ''||:v_fully_qualified_name||'' where ''||:v_data_archival_column||
                              '' < dateadd(''||:v_data_retention_date_time_part||'',-''||v_data_retention_value||'',current_date());''; 
       select "target_count" into v_target_rows from table(result_scan(last_query_id()));
       --delete data from snowflake table when actual records and unloaded records are same
       if(v_target_rows=v_rows_unloaded) then 
               execute immediate ''delete from ''||:v_fully_qualified_name||'' where ''||:v_data_archival_column||
                              '' < dateadd(''||:v_data_retention_date_time_part||'',-''||v_data_retention_value||'',current_date());''; 
              select "number of rows deleted" into v_rows_deleted from table(result_scan(last_query_id()));
       else
       raise v_exception_row_counts;
       end if; 

      
       --log ETL run as success
        call EA_STAGING.ETL.USP_LOG_ETL_SUCCESS(:RUNID,object_construct(''actual_rows'',:v_target_rows,''rows_unloaded'',:v_rows_unloaded,''rows_deleted'',:v_rows_deleted));
       
       
    else
       raise v_exception_req_val;
       
    end if;
    return ''success'';

   exception
     when other then
       begin 
          
           call EA_STAGING.ETL.USP_LOG_ETL_FAILURE(:RUNID
                                                   ,object_construct(
                                                   ''proc'',''EA_STAGING.ETL.USP_ARCHIVE_DATA(STRING,VARCHAR,VARCHAR,VARCHAR)''
                                                   ,''SQLCODE'',:SQLCODE
                                                   ,''SQLERRM'',:SQLERRM
                                                   ,''SQLSTATE'',:SQLSTATE
                                                   )
                                                  ,object_construct(''actual_rows'',:v_target_rows,''rows_unloaded'',:v_rows_unloaded,''rows_deleted'',:v_rows_deleted)
                                                  );
      raise v_exception;
      
       end;
  end;
end';
