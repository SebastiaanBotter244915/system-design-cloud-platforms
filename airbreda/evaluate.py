# evaluate.py
import pandas as pd
from sklearn.model_selection import LeaveOneOut, KFold, cross_val_predict
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.dummy import DummyRegressor

from predict import load_data, build_model

X, y = load_data()
n = len(y)
print(f"n = {n} samples\n")

# Leave-one-out for very small data, 5-fold otherwise
cv = LeaveOneOut() if n < 30 else KFold(n_splits=5, shuffle=True, random_state=42)

# Cross-validated predictions: every sample is predicted by a model that never saw it
y_pred = cross_val_predict(build_model(), X, y, cv=cv)
y_base = cross_val_predict(DummyRegressor(strategy="mean"), X, y, cv=cv)

print("--- Cross-validated performance ---")
print(f"Model    R²: {r2_score(y, y_pred):.3f}   MAE: {mean_absolute_error(y, y_pred):.2f} µg/m³")
print(f"Baseline R²: {r2_score(y, y_base):.3f}   MAE: {mean_absolute_error(y, y_base):.2f} µg/m³  (always predict the mean)")

# Coefficients from a model fit on all the data
model = build_model().fit(X, y)
print("\n--- Coefficients ---")
print(pd.Series(model.coef_, index=X.columns).round(4))
print("Intercept:", round(model.intercept_, 3))

print("\n--- Feature correlations ---")
print(pd.concat([X, y], axis=1).corr().round(2))