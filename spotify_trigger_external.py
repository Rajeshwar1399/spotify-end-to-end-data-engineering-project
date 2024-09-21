from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


from airflow.providers.amazon.aws.operators.glue import GlueJobOperator    #for glue jobs

default_args = {
    "owner":"rajeshwar",
    "depends_on_past":False,
    "start_date":datetime(2024,9,16)
}

dag = DAG(
    dag_id="spotify_trigger_external",
    default_args=default_args,
    description="DAG to trigger Lambda function and check S3 uploads",
    schedule_interval = timedelta(days=1),
    catchup = False
)


#trigger to extract using lambda function
trigger_extract_lambda = LambdaInvokeFunctionOperator(
    task_id="trigger_extract_lambda",
    function_name="spotify_api_data_extract",
    aws_conn_id="aws_spotify",
    region_name="us-east-1",
    dag=dag
)

# to check if the files are present in S3
check_s3_upload = S3KeySensor(
    task_id="check_s3_upload",
    bucket_key="s3://spotify-etl-project-rajeshwar/raw_data/to_processed/*.json",
    wildcard_match=True,                                    #considers the pattern of key valid
    aws_conn_id="aws_spotify",
    timeout=60 * 60,                                        #wait for one hour
    poke_interval=60,                                      #check every 60 sec
    dag=dag

)

#Trigger to transform using lambda function
# trigger_transform_lambda = LambdaInvokeFunctionOperator(
#     task_id="trigger_transform_lambda",
#     function_name="spotify_transformation_load_function",
#     aws_conn_id="aws_spotify",
#     region_name="us-east-1",
#     dag=dag
# )

#for triggering transformation in glue job
trigger_transform_glue = GlueJobOperator(
    task_id="trigger_transform_glue",
    job_name="spotify_transformation_job",
    script_location="s3://aws-glue-assets-381492215762-us-east-1/scripts/spotify_transformation_job.py",  #provide complete script path
    aws_conn_id="aws_spotify",
    region_name="us-east-1",
    iam_role_name="spotify_glue_iam_role",
    s3_bucket="s3://aws-glue-assets-381492215762-us-east-1",    #temporary bucket name to be provided
)


#trigger_extract_lambda >> check_s3_upload >> trigger_transform_lambda
trigger_extract_lambda >> check_s3_upload >> trigger_transform_glue


