#!/usr/bin/python 
# -*- coding: utf-8 -*-

# Load Libaries
from google.cloud import bigquery
from google.cloud import bigquery_storage
from google.cloud import storage
from datetime import datetime
from dateutil.relativedelta import relativedelta
from lifetimes import BetaGeoFitter, ParetoNBDFitter, GammaGammaFitter, utils
import math
import numpy as np
import os
import pandas as pd
import logging
import re
import config
import sys
from string import Template, capwords
import pyarrow


# Set variables
logger = logging.getLogger(__name__)
PENALIZER_COEF = config.config_vars['PENALIZER_COEF']
DISCOUNT_RATE = config.config_vars['DISCOUNT_RATE']
PREDICTION_LENGTH_IN_MONTHS = config.config_vars['PREDICTION_LENGTH_IN_MONTHS']
PREFIX=config.config_vars['PREFIX']
FREQUENZY= config.config_vars['FREQUENZY']
GCS_BUCKET_MODELS = config.config_vars['GCS_BUCKET_MODELS']
GCS_BUCKET_PREDICTIONS = config.config_vars['GCS_BUCKET_PREDICTIONS']
LOCAL_STORAGE_FOLDER = config.config_vars['LOCAL_STORAGE_FOLDER']
TRAINING_DATA_QUERY = config.config_vars['TRAINING_DATA_QUERY']
ACTUAL_CUSTOMER_VALUE_QUERY = config.config_vars['ACTUAL_CUSTOMER_VALUE_QUERY']
UPDATE_BIGQUERY_RESULT_TABLE = config.config_vars['UPDATE_BIGQUERY_RESULT_TABLE']




def file_to_string(sql_path):
    """Converts a SQL file holding a SQL query to a string.
    Args:
        sql_path: String containing a file path
    Returns:
        String representation of a file's contents
    """
    try:
        with open(sql_path, 'r') as sql_file:
            return sql_file.read()
    except Exception as error_message:
        logger.error("Fatal in error file_to_string function", exc_info=True)
    

# Function that loads data from Bigquery and creates a training dataset
def load_data_from_bq(training_data_query, actual_customer_value_query):
    """ Load data from Bigquery and creates a training dataset
    The Bigquery dataset should contain userId, prder_date and Order_value
    Args:
        training_data_query: Query that returns userId, order_date, order_value
        actual_customer_value_query: query that returns userId, current_total_revenue
    Returns: 
        training_df, actual_customer_value_df
    """
    try:
        #Load training data
        query = file_to_string(training_data_query)
        client = bigquery.Client()
        training_df = client.query(query).to_dataframe()

        # Load historical customer value
        query = file_to_string(actual_customer_value_query)
        client = bigquery.Client()
        actual_customer_value_df = client.query(query).to_dataframe()
        actual_customer_value_df = \
            actual_customer_value_df.set_index('userId')
        return (training_df, actual_customer_value_df)
    except Exception as error_message:
        logger.error("Fatal in error load_data_from_bq function", exc_info=True)
    


# Function that transforms data into RFM summary DF and actual_df
def transform_data(training_df, actual_customer_value_df, frequency='M'
                   ):
    """ transforms data into RFM summary DF and actual_df.
    Takes the two dataframes you have generated with load_data_from_bq
    as input
    Args:
        training_df: The dataset that will be transformed to summary table
        actual_customer_value_df: Information used for testing
    Returns: 
        
        summary, actual_df
    """
    try:
        logging.info('Loading data...')

        summary = utils.summary_data_from_transaction_data(training_df,
                'userId', 'order_date', monetary_value_col='order_value',
                freq=frequency)
        summary = summary[(summary['monetary_value'] > 0)
                        & (summary['frequency'] > 0)]
        actual_df = pd.merge(summary, actual_customer_value_df,
                            left_index=True, right_index=True)

        logging.info('Data loaded.')
        return (summary, actual_df)
    except Exception as error_message:
        logger.error("Fatal in error transform_data function", exc_info=True)



def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:
        a/1.txt
        a/b/2.txt

    If you just specify prefix = 'a', you'll get back:
        a/1.txt
        a/b/2.txt
    However, if you specify prefix='a' and delimiter='/', you'll get back:
        a/1.txt
    Additionally, the same request will return blobs.prefixes populated with:
        a/b/
    """
    try:
        storage_client = storage.Client()

        # Note: Client.list_blobs requires at least package version 1.17.0.
        blobs = storage_client.list_blobs(
            bucket_name, prefix=prefix, delimiter=delimiter
        )
        matching_files =  pd.DataFrame(columns=['file'])
    
        for blob in blobs:
            matching_files = matching_files.append({'file': blob.name}, ignore_index=True)
        
        return matching_files
    except Exception as error_message:
        logger.error("Fatal in error list_blobs_with_prefix function", exc_info=True)


def extract_date_from_string(string):
    """Extract date in yyyy-mm-dd format from string
    Args:
        String:      The string you want to retrive a date from
    Returns:
        Date:  A date in yyyy-mm-dd format
    """
    try:
        match = re.search(r'\d{4}-\d{2}-\d{2}', string)
        if match is not None:
            date = datetime.strptime(match.group(), '%Y-%m-%d').date()
        if 'date' not in locals():
            date = None
        return date
    except Exception as error_message:
        logger.error("Fatal in error extract_date_from_string function", exc_info=True)


def find_newest_models(df_with_file_names):
    """Locates the files from the latest date
    Args:
        list_of_clv_model_names = DF with file name column
        the file name must contain a date in yyyy-mm-dd
        format.
    Returns:
        Pandas series with files from the latest date
    """
    try:
        dates = []
        for file_names in df_with_file_names['file']:
            date = extract_date_from_string(file_names)
            dates.append(date)

        df_with_file_names = df_with_file_names.assign(date=dates)
        files_to_download = df_with_file_names[df_with_file_names['date'] == df_with_file_names['date'].max()]['file']
        return files_to_download
    except Exception as error_message:
        logger.error("Fatal in error find_newest_models function", exc_info=True)
    
    
def download_blob(bucket_name, 
                  source_blob_name, 
                  destination_file_name, 
                  destination_file_location):
    """Downloads a blob from a GCS bucket.
    Args:
        bucket_name = "your-bucket-name"
        source_blob_name = "storage-object-name"
        destination_file_name = "local/path/to/file"
    Returns:
        Downloads file to local storage
    """
    try:
        destination_file_path = destination_file_location+destination_file_name
        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_path)
        print(
            "Blob {} downloaded to {}.".format(
                source_blob_name, destination_file_path
            )
        )
    except Exception as error_message:
        logger.error("Fatal in error download_blob function", exc_info=True)

# Function that uploads local file to GCS
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket.
    Args:
        bucket_name: Your Google Cloud Storage bucket name
        source_file_name: path+filename of local file
        destination_blob_name: Name of file in Google Cloud Storage
    Returns: 
        blob_link: The uri of the file that has been uploaded
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_name)
        blob_link = 'gs://{}/{}'.format(bucket_name, destination_blob_name)
        return blob_link
    except Exception as error_message:
        logger.error("Fatal in error upload_blob function", exc_info=True)

# Function that uploads GCS CSV file to BQ
def upload_cloud_storage_csv_file_to_bq_table(blob_link, temporary_table_id):
    """Truncates BigQuery table with CSV file stored in Google Cloud Storage.
    Args:
        blob_link: The uri of the file that will be written to BigQuery
        temporary_table_id: The table is being overwritten with data from the CSV file.
        Make sure the provided table id does not contain any data that should not be overwriten.
    """
    try: 
        # Construct a BigQuery client object.
        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            autodetect=True, 
            skip_leading_rows=1, 
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV
        )
        load_job = client.load_table_from_uri(
            blob_link, temporary_table_id, job_config=job_config
        )  # Make an API request.
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(temporary_table_id)
        print("Loaded {} rows to {}.".format(destination_table.num_rows, temporary_table_id))
    except Exception as error_message:
        logger.error("Fatal in error upload_cloud_storage_csv_file_to_bq_table function", exc_info=True)

# Function that append dataframe to a BigQuery Table
def upload_new_predictions_to_bigquery(df,
                            gcs_bucket_predictions,
                            localFolderPath,
                            csv_file_name,
                            temporary_table_id = 'ml_models_production.new_predictions'):
    """Overwrites BigQuery table with data from dataframe
    Args:
        df: A dataframe with the same schema as destination table
        gcs_bucket_predictions: Google Cloud Storage bucket name that the CSV file with new predictions will be uploaded to.
        csv_file_name: The name of the csv file that will be created for GCS
        temporary_table_id: The table Id for a temporary table that will be overwritten. Is used for deduplication
    returns:
        The function first saves a local csv file, uploads it to GCS, writes 
        csv file to a temporary table in BigQuery.
    """
    try:
        # Save local file
        csv_file_path = localFolderPath+csv_file_name
        df.to_csv(csv_file_path, encoding="utf-8", index=False)
        #Upload local CSV file to GCS
        blob_link = upload_blob(bucket_name=gcs_bucket_predictions,
                                source_file_name = csv_file_path,
                                destination_blob_name = csv_file_name)
        # Upload CSV file from GCS to temporary BQ table
        upload_cloud_storage_csv_file_to_bq_table(blob_link, temporary_table_id)
    except Exception as error_message:
        logger.error("Fatal in error upload_new_predictions_to_bigquery function", exc_info=True)



# Function that updates or adds new predictions to clv_and_churn_predictions table
def update_or_add_new_predictions_to_clv_and_churn_predictions_table(sql_path):
    """ updates or adds new predictions to clv_and_churn_predictions table"""
    try:

        # Update CLV segmentation and Churn probability segmentation
        query = file_to_string(sql_path)
        client = bigquery.Client()
        client.query(query)
    except Exception as error_message:
        logger.error("Fatal in error update_or_add_new_predictions_to_clv_and_churn_predictions_table function", exc_info=True)


def predict_value(
    summary,
    actual_df,
    fitter,
    ggf,
    t,
    time_months,
    discount_rate,
    frequency
    ):
    """Predict lifetime values for customers.
    Args:
        summary:      RFM transaction data
        actual_df:    dataframe containing data fields for customer id,
                      actual customer values
        fitter:       lifetimes fitter, previously fit to data
        ggf:          lifetimes gamma/gamma fitter, already fit to data
        time_days:    time to predict purchases in days
        time_months:  time to predict value in months
    Returns:
        ltv:  dataframe with predicted values for each customer, along with actual
        values and error
        rmse: root mean squared error summed over all customers
    """
    try: 
        # setup dataframe to hold results
        ltv = actual_df

        predicted_num_purchases = \
            fitter.conditional_expected_number_of_purchases_up_to_time(t,
                summary['frequency'], summary['recency'], summary['T'])

        p_alive = fitter.conditional_probability_alive(summary['frequency'
                ], summary['recency'], summary['T'])

        predicted_value = ggf.customer_lifetime_value(
            fitter,
            summary['frequency'],
            summary['recency'],
            summary['T'],
            summary['monetary_value'],
            time=time_months,
            discount_rate=discount_rate,
            freq=frequency,
            )

        # Create ltv table with predicted values

        predicted_value.rename('predicted_value_next_6_month', inplace=True)
        ltv.insert(0, 'userId', ltv.index)
        ltv = pd.merge(ltv, predicted_value, left_index=True,
                    right_index=True)
        predicted_num_purchases.rename('predicted_transactions_next_6_month'
                                    , inplace=True)
        ltv = pd.merge(ltv, predicted_num_purchases, left_index=True,
                    right_index=True)
        ltv['predicted_total'] = ltv['current_total_revenue'] \
            + ltv['predicted_value_next_6_month']
        ltv.reset_index(drop=True, inplace=True)
        p_alive = pd.Series(p_alive).rename('p_alive', inplace=True)
        churn = 1 - p_alive
        ltv['churn_probability'] = pd.Series(churn, index=ltv.index)


        model_output = ltv[['userId', 'predicted_total', 'churn_probability'
                        , 'predicted_value_next_6_month',
                        'current_total_revenue']].copy()

        model_output.columns = ['userId', 'clv', 'churn_probability',
                                'predicted_value_next_6_month',
                                'current_total_revenue']
        # Set number of decimals
        model_output.loc[:, model_output.columns != 'churn_probability'] = \
        model_output.loc[:, model_output.columns != 'churn_probability'].round(2)
        model_output['churn_probability'] = model_output['churn_probability'].round(4)
        return model_output
    except Exception as error_message:
        logger.error("Fatal in error predict_value function", exc_info=True)


def run_btyd(
    training_data_query,
    actual_customer_value_query,
    prediction_length_in_months,
    gcs_bucket_models,
    gcs_bucket_predictions,
    prefix, 
    local_storage_folder,
    frequency='M',
    penalizer_coef=0,
    discount_rate=0.01):
    """Run selected BTYD model on data loaded from BigQuery and save model to GCS and predictions to BQ
  Args:
        training_data_query:        Query that returns userId, order_date, order_value
        actual_customer_value_query:Query that returns userId, current_total_revenue
        prediction_length_in_months:The number of month you want to predict
        gcs_bucket_models:          The name of the bucket your models are stored in
        gcs_bucket_predictions:     The name of the bucket your new predictions are stored in
        prefix:                     Prefix to model names that should be loaded
        local_storage_folder:       The local folder in your system you want to store the models in temporary
        frequency:                  The frequency used to calculate your summary table
        penalizer_coef:             Penalizer used in fitter and ggf models
        discount_rate:              Used to discount future revenue to current day value
  """
    try:
        (training_df, actual_customer_value_df) = load_data_from_bq(training_data_query,
                                                                    actual_customer_value_query)
        
        if (training_df.empty or actual_customer_value_df.empty):
            sys.exit('No new customers to calculate CLV for / BigQuery did not return any results. Script will not continue to run')

        # load training transaction data

        (summary, actual_df) = transform_data(training_df,
                actual_customer_value_df, frequency)

        # Find newest trained fitter and ggf model in GCS and download.
        clv_models = list_blobs_with_prefix(gcs_bucket_models, prefix)
        files_to_download = find_newest_models(clv_models)
        for file in files_to_download:
            download_blob(gcs_bucket_models, 
                        file, 
                        file, 
                        local_storage_folder)
    # Load fitter and ggf model for last trained model
        logging.info('Loading model...')

        for file in files_to_download:
            if 'BGNBD' in file:
                fitter = BetaGeoFitter(penalizer_coef=penalizer_coef)
                fitter.load_model(local_storage_folder+file)
            if 'PARETO' in file:
                fitter = ParetoNBDFitter(penalizer_coef=penalizer_coef)
                fitter.load_model(local_storage_folder+file)
            if 'ggf' in file:
                ggf = GammaGammaFitter(penalizer_coef=penalizer_coef)
                ggf.load_model(local_storage_folder+file)

        logging.info('Done.')

    
        # use loaded fitter to predicted ltv for each user
    
        # compute the number of days in the prediction period

        if frequency == 'D':
            t = prediction_length_in_months/30
            time_months = prediction_length_in_months
        elif frequency == 'w':
            t = prediction_length_in_months/4
            time_months = prediction_length_in_months
        elif frequency == 'M':
            t = prediction_length_in_months
            time_months = prediction_length_in_months
            
        else:
            logging.error('Please either choose D, W or M as input for freuency'
                        )
            print('Please either choose D, W or M as input for freuency')

        # Get new predictions
        model_output = predict_value(summary,
                                    actual_df,
                                    fitter,
                                    ggf,
                                    t,
                                    time_months,
                                    discount_rate,
                                    frequency)

        # Upload model predictions to temporary BigQuery table
        today = datetime.today().strftime("%Y%m%d")
        csv_file_name = 'daily_predictions_'+today+'.csv'
        upload_new_predictions_to_bigquery(model_output,
                                            gcs_bucket_predictions,
                                            local_storage_folder,
                                            csv_file_name,
                                            'ml_models_production.new_predictions')
        
        # Add new predictions to the clv_and_churn_prediction table and update segments
        update_or_add_new_predictions_to_clv_and_churn_predictions_table(UPDATE_BIGQUERY_RESULT_TABLE)

        logging.info('CLV and Churn Predections has been uploaded to BigQuery')
    except Exception as error_message:
        logger.error("Fatal in error run_btyd function", exc_info=True)


def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    try:
        current_time = datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            run_btyd(TRAINING_DATA_QUERY,
                     ACTUAL_CUSTOMER_VALUE_QUERY,
                     PREDICTION_LENGTH_IN_MONTHS,
                     GCS_BUCKET_MODELS,
                     GCS_BUCKET_PREDICTIONS,
                     PREFIX,
                     LOCAL_STORAGE_FOLDER,
                     FREQUENZY,
                     PENALIZER_COEF,
                     DISCOUNT_RATE)           

        except Exception as error:
            log_message = Template('Predictions failed due to '
                                   '$message.')
            logging.error(log_message.safe_substitute(message=error))

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)


