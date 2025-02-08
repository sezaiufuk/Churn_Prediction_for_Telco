from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, size
import os
import pandas as pd
from sklearn.model_selection import train_test_split, StratifiedKFold
import numpy as np
from sklearn.utils.class_weight import compute_class_weight
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
import lightgbm as lgb
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import ADASYN
import joblib

#region [Starter]

spark = SparkSession.builder \
    .appName("GuardiansofChurn") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
folder_path = "./data"
files = [f for f in os.listdir(folder_path) if f.endswith(".jsonl")]
data = None

for file in files:
    file_path = os.path.join(folder_path, file)
    df = spark.read.json(file_path)
    
    if data is None:
        data = df
    else:
        data = data.union(df)

data.show(truncate=False)

#endregion

#region [Helpers]

def checkChurnBalance(df, churn_column='Churn'):
    
    churn_counts = df.groupBy(churn_column).count().collect()
    
    for row in churn_counts:
        print(f"{row[churn_column]}: {row['count']} records")

def getNullCount(df):
    null_counts = df.select([
        sum(col(c).isNull().cast("int")).alias(c + "_null_count") for c in df.columns
    ])

    null_counts.show()

def do_adasyn(target):
    X = target.drop("churn", axis=1)
    y = target["churn"]

    adasyn = ADASYN(sampling_strategy='auto', random_state=42)
    X_resampled, y_resampled = adasyn.fit_resample(X, y)
    
    return pd.concat([pd.DataFrame(X_resampled, columns=X.columns), pd.DataFrame(y_resampled, columns=["churn"])], axis=1)

def toPandas(df, chunk_size=100_000):
    total_rows = df.count()
    chunk_list = []

    for offset in range(0, total_rows, chunk_size):
        chunk = df.limit(chunk_size).offset(offset)
        chunk_list.append(chunk.toPandas())

    return pd.concat(chunk_list, ignore_index=True)


def preprocess_and_split(data, pca_components=5, test_size=0.2, random_state=42):
    X = data.drop("churn", axis=1)
    y = data["churn"]

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    train_data_resampled = do_adasyn(pd.concat([X_train, y_train], axis=1))
    
    print(f"Original train data shape: {X_train.shape}")
    print(f"Resampled train data shape: {train_data_resampled.shape}")
    
    X_train_resampled = train_data_resampled.drop("churn", axis=1)
    y_train_resampled = train_data_resampled["churn"]

    scaler = StandardScaler()
    X_train = pd.DataFrame(scaler.fit_transform(X_train_resampled), columns=X_train_resampled.columns)
    X_test = pd.DataFrame(scaler.transform(X_test), columns=X_test.columns)

    pca = PCA(n_components=pca_components)
    X_train = pca.fit_transform(X_train)
    X_test = pca.transform(X_test)

    # variance_ratio = pca.explained_variance_ratio_
    # print(f"Explained variance ratio: {variance_ratio}")
    
    return X_train, X_test, y_train_resampled, y_test


#endregion

#region [Data > Column Operations]

data = data.withColumn("tenure", when(col("tenure").isNull(), 0).otherwise(col("tenure")))

data = data.drop(col("auto_payment"))

data = data.withColumn("data_usage", when(col("data_usage").isNull(), 0).otherwise(col("data_usage")))

data = data.withColumn("monthly_charge", when(col("monthly_charge").isNull(), 0).otherwise(col("monthly_charge")))

data = data.withColumn("avg_call_duration", when(col("avg_call_duration").isNull(), 0).otherwise(col("avg_call_duration")))

data = data.withColumn("churn", col("churn").cast("int"))

data = data.withColumn('total_apps_used', size(col('apps')))

data = data.drop(col("apps"))

data = data.withColumn('charge_support_call_ratio', data['monthly_charge'] / (data['customer_support_calls']+1))

data = data.withColumn('charge_per_usage', data['monthly_charge'] / (data['data_usage']+1))

data = data.withColumn('sup_call_per_month', data['customer_support_calls'] / (data['tenure']+1))

data = data.withColumn('overdue_ratio', data['overdue_payments'] / (data['tenure']+1))

data = data.withColumn(
    "payment_consistency_score",
    (col("monthly_charge") / (col("overdue_payments") + 1)),
)

data = data.withColumn(
    "app_usage_charge_ratio",
    col("total_apps_used") / (col("monthly_charge") + 1)
)

data = data.withColumn(
    "app_usage_charge_interaction",
    col("total_apps_used") * col("monthly_charge")
)

#endregion

#region [Branches of the Problem]

prepaid_service_data = data.select("*").where(col("service_type") == "Prepaid")
postpaid_service_data = data.select("*").where(col("service_type") == "Postpaid")
broadband_service_data = data.select("*").where(col("service_type") == "Broadband")

#endregion

#region [Services > Broadband]

columns_to_drop_broadband = ["id","avg_call_duration", "service_type", "roaming_usage",
                   "call_drops","avg_top_up_count","total_apps_used"]

broadband_service_data = broadband_service_data.drop(*columns_to_drop_broadband)

getNullCount(broadband_service_data)

#endregion

#region [Services > Prepaid]

columns_to_drop_prepaid = ["id","overdue_payments","service_type",
                           "overdue_ratio",'payment_consistency_score']

prepaid_service_data = prepaid_service_data.drop(*columns_to_drop_prepaid)

getNullCount(prepaid_service_data)

#endregion


#region [Services > Postpaid]

columns_to_drop_postpaid = ["id","avg_top_up_count","service_type"]

postpaid_service_data = postpaid_service_data.drop(*columns_to_drop_postpaid)

getNullCount(postpaid_service_data)

#endregion

#region [Starting to Modelling]

broadband_service_data_pandas = toPandas(broadband_service_data)
prepaid_service_data_pandas = toPandas(prepaid_service_data)
postpaid_service_data_pandas = toPandas(postpaid_service_data)

X_train_broadband, X_test_broadband, y_train_broadband, y_test_broadband = preprocess_and_split(broadband_service_data_pandas)
X_train_prepaid, X_test_prepaid, y_train_prepaid, y_test_prepaid = preprocess_and_split(prepaid_service_data_pandas)
X_train_postpaid, X_test_postpaid, y_train_postpaid, y_test_postpaid = preprocess_and_split(postpaid_service_data_pandas)

#region [Model]

from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, classification_report
import numpy as np
import lightgbm as lgb

def train_and_evaluate_model(X_train, y_train, X_test, y_test, threshold=0.15):
    if isinstance(X_train, np.ndarray):  # Eğer NumPy array ise dönüştür
        X_train = pd.DataFrame(X_train)
        X_test = pd.DataFrame(X_test)

    if isinstance(y_train, np.ndarray):
        y_train = pd.Series(y_train)
        y_test = pd.Series(y_test)
    scale_pos_weight = (len(y_train) - y_train.sum()) / y_train.sum()

    lgbm = lgb.LGBMClassifier(
        n_estimators=2000,
        learning_rate=0.01,
        max_depth=50,
        random_state=42,
        n_jobs=-1,
        scale_pos_weight=scale_pos_weight
    )

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    auc_scores = []

    for train_index, val_index in skf.split(X_train, y_train):
        X_train_fold, X_val_fold = X_train.iloc[train_index], X_train.iloc[val_index]
        y_train_fold, y_val_fold = y_train.iloc[train_index], y_train.iloc[val_index]

        lgbm.fit(X_train_fold, y_train_fold)

        y_pred_proba = lgbm.predict_proba(X_val_fold)[:, 1]
        auc = roc_auc_score(y_val_fold, y_pred_proba)
        auc_scores.append(auc)

    print(f"Ortalama AUC: {np.mean(auc_scores):.4f} ± {np.std(auc_scores):.4f}")

    lgbm.fit(X_train, y_train)

    y_pred_proba_test = lgbm.predict_proba(X_test)[:, 1]
    y_pred_test = (y_pred_proba_test > threshold).astype(int)

    print(classification_report(y_test, y_pred_test))
    
    return lgbm

broadband_model = train_and_evaluate_model(X_train_broadband, y_train_broadband, X_test_broadband, y_test_broadband)
joblib.dump(broadband_model, "./models/broadband/lgbm_model_broadband_2.pkl")

prepaid_model = train_and_evaluate_model(X_train_prepaid, y_train_prepaid, X_test_prepaid, y_test_prepaid)
joblib.dump(prepaid_model, "./models/prepaid/lgbm_model_prepaid_2.pkl")

postpaid_model = train_and_evaluate_model(X_train_postpaid, y_train_postpaid, X_test_postpaid, y_test_postpaid)
joblib.dump(prepaid_model, "./models/postpaid/lgbm_model_postpaid_2.pkl")

# loaded_lgbm = lgb.Booster(model_file="./models/broadband/lgbm_model_broadband.txt")
# loaded_lgbm = joblib.load("./models/broadband/lgbm_model_broadband.pkl")
# y_pred = loaded_lgbm.predict(X_test_broadband)
# y_pred = (y_pred > 0.1).astype(int)
# print(classification_report(y_test_broadband, y_pred_test))

#data.des

broadband_service_data_pandas.describe()
prepaid_service_data_pandas.describe()
postpaid_service_data_pandas.describe()



