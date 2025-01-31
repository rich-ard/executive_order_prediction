
# retrieve data from BigQuery
import os
import configparser
from datetime import datetime
from google.cloud import bigquery
import itertools
import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
import warnings
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from statsmodels.tools.sm_exceptions import ValueWarning, ConvergenceWarning
warnings.simplefilter('ignore', ValueWarning)
warnings.simplefilter('ignore', ConvergenceWarning)
# https://stackoverflow.com/questions/49547245/valuewarning-no-frequency-information-was-provided-so-inferred-frequency-ms-wi
import statsmodels.api as sm

def load_env_values() -> None:
    config = configparser.ConfigParser(interpolation=None)
    config.sections()
    config.read(".env")
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["google"]["GOOGLE_APPLICATION_CREDENTIALS"]
    os.environ["MLFLOW_TRACKING_USERNAME"] = config["mlflow"]["MLFLOW_TRACKING_USERNAME"]
    os.environ["MLFLOW_TRACKING_PASSWORD"] = config["mlflow"]["MLFLOW_TRACKING_PASSWORD"]
    os.environ["MLFLOW_TRACKING_URI"] = config["mlflow"]["MLFLOW_TRACKING_URI"]

client = bigquery.Client()

query = """
DECLARE MaxDate DATETIME
  DEFAULT (
    SELECT MAX(run_date)
    FROM `executive-orders-448515.weekly_data_collected.weekly_variables_flattened`
  );

SELECT 
  * EXCEPT(run_date)
FROM `executive-orders-448515.weekly_data_collected.weekly_variables_flattened`
WHERE run_date = MaxDate
ORDER BY week_start ASC;
"""

raw_data = client.query_and_wait(query)
df = raw_data.to_dataframe()

# index rows by week
df['week_start'] = pd.to_datetime(df['week_start'])
df = df.sort_values(by='week_start', ascending=True)
df.set_index('week_start', inplace=True)

# replace outcome var outliers with median
median = df['orders_outcome_var'].median()
std = df['orders_outcome_var'].std()
df.loc[(df['orders_outcome_var'] - median).abs() > 3*std]=np.nan
df.fillna({'orders_outcome_var':median}, inplace = True)

# spline-interpolate exog vars to fill missing values
df_exog_interpolated = df.drop('orders_outcome_var', axis=1).astype(float).interpolate(method='spline', order = 3) # order 3 is cubic

# create principal components from exog vars
scaler = StandardScaler()
df_to_standardize = df_exog_interpolated.drop(['disapproving'], axis=1)
df_standardized = scaler.fit_transform(df_to_standardize)

pca = PCA( n_components=4) # data exploration determined that 4 PCs will be retained
principal_components = pca.fit_transform(df_standardized)
df_principal_components = pd.DataFrame(principal_components, columns=['PC1','PC2','PC3','PC4'], index=df_to_standardize.index)

df1 = df_principal_components.join(df['orders_outcome_var'])

# Split train and test data 80/20
df_train = df1[:round(len(df1)*.8)]
df_test = df1[round(len(df1)*.8):]

# Hyperparameters tuning
p = d = q = range(0, 2)
pdq = list(itertools.product(p, d, q))
seasonal_pdq = [(x[0], x[1], x[2], 52) for x in list(itertools.product(p, d, q))] #52 here represents weeks

# print('Examples of parameter combinations for Seasonal ARIMA...')
# print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[1]))
# print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[2]))
# print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[3]))
# print('SARIMAX: {} x {}'.format(pdq[1], seasonal_pdq[4]))
# print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[1]))
# print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[2]))
# print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[3]))
# print('SARIMAX: {} x {}'.format(pdq[2], seasonal_pdq[4]))

df_results = pd.DataFrame(columns=['pdq', 'seasonal_pdq', 'AIC'])
for param in pdq:
    for param_seasonal in seasonal_pdq:
        try:
            mod = sm.tsa.statespace.SARIMAX(endog=df_train['orders_outcome_var'],
                                            exog=df_train.drop(['orders_outcome_var'], axis=1),
                                            order=param,
                                            seasonal_order=param_seasonal,
                                            enforce_stationarity=True,
                                            enforce_invertibility=True)
            results = mod.fit(disp=False)
            df_results.loc[len(df_results)] = [param, param_seasonal, results.aic]
            print('ARIMA{}x{}12 - AIC:{}'.format(param, param_seasonal, results.aic))
        except Exception as e:
            print(f'Error: {e}')
            continue

# following model testing, choose order and seasonal order with lowest AIC
final_sarimax_model_params = df_results.iloc[df_results['AIC'].idxmin()]
print(final_sarimax_model_params['pdq'])
print(final_sarimax_model_params)

final_model = sm.tsa.statespace.SARIMAX(endog=df_train['orders_outcome_var'],
                                        exog=df_train.drop(['orders_outcome_var'], axis=1),
                                        order=final_sarimax_model_params['pdq'],
                                        seasonal_order=final_sarimax_model_params['seasonal_pdq'],
                                        enforce_stationarity=True,
                                        enforce_invertibility=True)

final_results = final_model.fit(disp=False)
print(final_results.summary().tables[1])

# Plot the forecasting results
pred = final_results.get_prediction(start=len(df_train['orders_outcome_var']),
                                    end=len(df_train['orders_outcome_var']) + len(df_test['orders_outcome_var']) -1,
                                    exog=df_test.drop(['orders_outcome_var'], axis = 1))
pred_ci = pred.conf_int() # Extract the confidence intervals for the predictions
ax = df_test['orders_outcome_var'].plot(label='observed')
pred.predicted_mean.plot(ax=ax, label='One-step ahead forecast', alpha=.7, figsize=(12, 7))
fig1, ax = plt.subplots()
ax = df_test['orders_outcome_var'].plot(label='observed')
pred.predicted_mean.plot(ax=ax, label='One-step ahead forecast', alpha=.7, figsize=(12, 7))
ax.fill_between(pred_ci.index,
                pred_ci.iloc[:, 0],
                pred_ci.iloc[:, 1], color='k', alpha=.2)
ax.set_xlabel('Date')
ax.set_ylabel('Executive Orders')
fig1.legend()
fig1.show()

# write predicted values to a date-partitioned GCP BigQuery table
output_to_bq = pd.DataFrame(pred.predicted_mean)
output_to_bq.reset_index(inplace=True)
output_to_bq['bq_load_dt'] = datetime.today()
output_to_bq.rename(columns={'index':'week_start','predicted_mean':'predicted_orders'}, inplace=True)
print(output_to_bq)

table_id = 'weekly_data_predicted.executive_order_count'

job_config = bigquery.LoadJobConfig(
    schema=[bigquery.SchemaField("week_start", "DATE"),
            bigquery.SchemaField("predicted_orders", "FLOAT64"),
            bigquery.SchemaField("bq_load_dt", "DATE")],
    write_disposition="WRITE_APPEND")

job = client.load_table_from_dataframe(
    output_to_bq, table_id, job_config=job_config
)

# Wait for the load job to complete. (I omit this step)
job.result()

# write experiment results to mlflow

model_name = f"SARIMAX_run_at{datetime.now()}"
load_env_values()
experiment = mlflow.get_experiment_by_name("SARIMAX")
with mlflow.start_run(run_name="Sarimax",
                      experiment_id=experiment.experiment_id) as run:
    mlflow.statsmodels.log_model(results,model_name,registered_model_name=model_name)
    mlflow.log_params({"order":final_sarimax_model_params['pdq'],
                       "seasonal_order":final_sarimax_model_params['seasonal_pdq'],
                       "AIC":final_results.aic})
    mlflow.log_figure(fig1, 'forecasting_results.png')
    model_uri = f"runs:/{run.info.run_id}/{model_name}"
    print("Model saved in run %s" % run.info.run_id)
    print(f"Model URI: {model_uri}")
    print('it worked')
mlflow.end_run()
