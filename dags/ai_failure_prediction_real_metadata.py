from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
import psycopg2
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib
from datetime import datetime, timedelta

MODEL_PATH = "/opt/airflow/models/failure_model_mtdb.pkl"

# Step 1 — Fetch historical data from Airflow metadata DB
def fetch_metadata():
    conn = BaseHook.get_connection("airflow_db")
    query = """
        SELECT dag_id, task_id,state,duration,try_number,EXTRACT(DOW FROM start_date) AS day_of_week
        FROM task_instance
        WHERE start_date IS NOT NULL
        AND duration IS NOT NULL
        AND dag_id NOT LIKE 'example_%'
        AND state IN ('success', 'failed')
        LIMIT 5000;
    """
    client = psycopg2.connect(
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        host=conn.host,
        port=conn.port
    )
    df = pd.DataFrame()
    try:
        cur = client.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)
        cur.close()
    except Exception as e:
        print(f"Exception: {e}")
    finally:
        client.close()

    print(f"Retrieved {len(df)} task records from Airflow DB.")
    df.to_csv("/opt/airflow/models/task_history_live.csv", index=False)
    return df.to_dict()

# Step 2 — Train or update the model
def train_model(context):
    #df = pd.DataFrame(context["ti"].xcom_pull(task_ids="fetch_metadata"))
    df = pd.read_csv('/opt/airflow/models/task_history_live.csv')
    if df.empty:
        print("No task history found.")
        return
    
    df["status"] = df["state"].apply(lambda x: 1 if x == "failed" else 0)
    X = df[["duration", "try_number", "day_of_week"]]
    y = df["status"]

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)
    joblib.dump(model, MODEL_PATH)

# Step 3 — Predict next run’s failure probability
def predict_failure():
    model = joblib.load(MODEL_PATH)
    # Example: new task metadata (could come from external job metrics)
    new_data = pd.DataFrame([{
        "duration": 200,
        "try_number": 1,
        "day_of_week": datetime.now().weekday()
    }])

    prob = model.predict_proba(new_data)[0][1]
    print(f"Predicted failure probability: {prob:.2f}")

    if prob > 0.6:
        print("High chance of failure detected!")
    else:
        print("Low risk — pipeline is healthy.")


# Step 4 — Define DAG
@dag(
    start_date=datetime(2025, 11, 8),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["AI", "DataOps", "Monitoring"],
)
def ai_failure_prediction_real_metadata():
    
    @task
    def fetch():
        fetch_metadata()
    
    @task
    def train(**context):
        train_model(context)
    
    @task
    def predict():
        predict_failure()

    fetch() >> train() >> predict() 

ai_failure_prediction_real_metadata()