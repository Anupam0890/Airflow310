# train_failure_model.py
import pandas as pd
#from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import joblib

# Load task history
df = pd.read_csv("./task_history.csv")

# Convert status to binary
df["status"] = df["status"].apply(lambda x: 1 if x == "failed" else 0)

# Features
X = df[["duration", "retries", "day_of_week"]]
y = df["status"]

# Train model
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X, y)

# Save model
joblib.dump(model, "./failure_model.pkl")

print("✅ Model trained and saved!")
