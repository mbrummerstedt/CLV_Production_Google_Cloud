config_vars = {
    # Set Variables that will be used in script
    'PENALIZER_COEF': 0.03,
    'DISCOUNT_RATE': 0.01,
    'PREDICTION_LENGTH_IN_MONTHS': 6,
    'PREFIX':'clv_model',
    'FREQUENZY':'M',
    'GCS_BUCKET_MODELS': 'your_company_trained_ml_models_production',
    'GCS_BUCKET_PREDICTIONS': 'your_company_ml_models_predictions',
    'LOCAL_STORAGE_FOLDER': '/tmp/',
    'TRAINING_DATA_QUERY': 'CLV-dataset-daily-predictions.sql',
    'ACTUAL_CUSTOMER_VALUE_QUERY': 'CLV-dataset-daily-predictions-customer-summary.sql',
    'UPDATE_BIGQUERY_RESULT_TABLE': 'CLV-daily-update-result-bigquery-table.sql'

    }
