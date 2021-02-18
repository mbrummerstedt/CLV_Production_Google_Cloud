config_vars = {
    # Set Variables that will be used in script
    'PENALIZER_COEF': 0.03,
    'DISCOUNT_RATE': 0.01,
    'PREDICTION_LENGTH_IN_MONTHS': 6,
    'MODEL_TYPE':'BGNBD',
    'FREQUENZY':'M',
    'GCS_BUCKET_MODELS': 'stylepit_trained_ml_models_production',
    'GCS_BUCKET_PREDICTIONS': 'stylepit_ml_models_predictions',
    'LOCAL_STORAGE_FOLDER': '/tmp/',
    'TRAINING_DATA_QUERY': 'CLV-dataset-weekly-training-and-prediction.sql',
    'ACTUAL_CUSTOMER_VALUE_QUERY': 'CLV-dataset-weekly-training-and-prediction-customer-summary.sql',
    'UPDATE_BIGQUERY_RESULT_TABLE': 'CLV-weekly-update-result-bigquery-table.sql'
    }
