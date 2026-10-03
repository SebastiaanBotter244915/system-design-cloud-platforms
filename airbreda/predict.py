# predict.py
from pathlib import Path

from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd
import joblib

FEATURES = ["total_intensity_veh_per_hr", "hour_of_day"]
TARGET = "no2_ug_m3"
MODEL_PATH = Path(__file__).parent / "model" / "model.pkl"

# Exceedance threshold in µg/m³ — EU annual limit value (see ADR-006).
# Note: this is an annual-average limit applied to hourly predictions.
NO2_THRESHOLD = 40.0

def load_data(path="training_data.csv"):
    df = pd.read_csv(path)
    return df[FEATURES], df[TARGET]

def build_model():
    return LinearRegression()

def exceedance_risk(predicted_no2, threshold=NO2_THRESHOLD, steepness=0.2):
    return float(1 / (1 + np.exp(-steepness * (predicted_no2 - threshold))))

def load_model():
    return joblib.load(MODEL_PATH)

def predict(total_intensity_veh_per_hr, hour_of_day):
    X = pd.DataFrame([[total_intensity_veh_per_hr, hour_of_day]], columns=FEATURES)
    no2 = float(load_model().predict(X)[0])
    return {
        "no2_ug_m3_predicted": no2,
        "no2_exceedance_risk": exceedance_risk(no2),
    }

if __name__ == "__main__":
    X, y = load_data()
    model = build_model()
    model.fit(X, y)
    joblib.dump(model, MODEL_PATH)
