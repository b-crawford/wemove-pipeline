import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

#######################################
#
# Setup
#
######################################

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "s3-upload-bucket", "s3-upload-object-key", "s3-output-bucket"],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")

#######################################
#
# ETL functions
#
######################################


def load_csv_in_input_folder(input_path, timestamp_cols, timestamp_format):
    # if the path contains spaces these will be replaced with plusses by the lambda
    # function to pass through as arguments, we unreplace them here.
    input_path = input_path.replace("+", " ")

    # load the dataframe
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # convert timestamp columns to datetime
    for col in timestamp_cols:
        df = df.withColumn(col, F.unix_timestamp(df[col], format=timestamp_format))

    return df


def ride_requests_transform(df, costs_df):
    # create actual rr time
    df = df.withColumn(
        "actual_rr_datetime",
        F.when(
            F.col("Requested Pickup Time").isNotNull(), F.col("Requested Pickup Time")
        ).otherwise(F.col("Requested Dropoff Time")),
    )

    # calculate time difference of planned picked and actual pickup
    df = df.withColumn(
        "time_difference_seconds",
        (df["Actual Pickup Time"] - df["Original Planned Pickup Time"]).cast("long"),
    )

    # create a late flag column
    df = df.withColumn("late_flag", F.col("time_difference_seconds") > 0)

    # join cost codes
    df = df.join(costs_df, on="Request Zone", how="left")

    return df


def pricing_and_payments_transform(df):
    # convert paid amount to pounds
    df = df.withColumn("Paid Amount", df["Paid Amount"] / 100)

    return df


def combine_with_existing_output(
    extracted_df,
    output_path,
    sorting_timestamp_col,
    other_timestamp_cols,
    unique_row_id_col,
):
    # load existing data (if any)
    try:
        existing_output = spark.read.csv(output_path, header=True, inferSchema=True)
        combined = existing_output.unionAll(extracted_df)
    except AnalysisException:
        print("No existing data")
        combined = extracted_df

    output = combined.sort(F.col(sorting_timestamp_col).desc()).dropDuplicates(
        [unique_row_id_col]
    )

    # save output dates to strings:
    for col in other_timestamp_cols + [sorting_timestamp_col]:
        output = output.withColumn(
            col, F.date_format(F.to_timestamp(F.col(col)), "yyyy-MM-dd HH:mm:ss")
        )

    output.repartition(1).write.csv(output_path, header=True, mode="overwrite")


def pipeline(
    input_path,
    output_path,
    timestamp_format,
    sorting_timestamp_col,
    other_timestamp_cols,
    unique_row_id_col,
    transform_function=lambda x: x,
):
    # load csvs
    df = load_csv_in_input_folder(
        input_path=input_path,
        timestamp_cols=[sorting_timestamp_col] + other_timestamp_cols,
        timestamp_format=timestamp_format,
    )

    # transform
    df = transform_function(df)

    # combine with existing output
    combine_with_existing_output(
        extracted_df=df,
        output_path=output_path,
        sorting_timestamp_col=sorting_timestamp_col,
        other_timestamp_cols=other_timestamp_cols,
        unique_row_id_col=unique_row_id_col,
    )


#######################################
#
# Execution
#
######################################

if "ride-requests" in args["s3_upload_object_key"]:
    print(f'Processing ride requests for {args["s3_upload_bucket"]}')

    costs_df = spark.read.csv(
        f"s3a://{args['s3_upload_bucket']}/costs_df.csv", header=True, inferSchema=True
    )

    pipeline(
        input_path=f"s3a://{args['s3_upload_bucket']}/{args['s3_upload_object_key']}",
        output_path=f"s3a://{args['s3_output_bucket']}/ride_requests",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        sorting_timestamp_col="Request Creation Time",
        other_timestamp_cols=[
            "Requested Pickup Time",
            "Requested Dropoff Time",
            "Original Planned Pickup Time",
            "Actual Pickup Time",
        ],
        unique_row_id_col="Request ID",
        transform_function=lambda x: ride_requests_transform(x, costs_df),
    )
elif "pricing-and-payments" in args["s3_upload_object_key"]:
    print(f'Processing pricing-and-payments for {args["s3_upload_bucket"]}')

    pipeline(
        input_path=f"s3a://{args['s3_upload_bucket']}/{args['s3_upload_object_key']}",
        output_path=f"s3a://{args['s3_output_bucket']}/pricing_and_payments",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        sorting_timestamp_col="Latest Payment Time",
        other_timestamp_cols=[],
        unique_row_id_col="Request ID",
        transform_function=pricing_and_payments_transform,
    )
elif "driver-shifts" in args["s3_upload_object_key"]:
    print(f'Processing driver shifts for {args["s3_upload_bucket"]}')

    pipeline(
        input_path=f"s3a://{args['s3_upload_bucket']}/{args['s3_upload_object_key']}",
        output_path=f"s3a://{args['s3_output_bucket']}/driver_shifts",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        sorting_timestamp_col="Date",
        other_timestamp_cols=[
            "Planned Shift Start Time",
            "Shift Start Time",
            "Planned Shift End Time",
            "Shift End Time",
        ],
        unique_row_id_col="Shift ID",
    )
else:
    print("No matching bucket found. Exiting...")


job.commit()
