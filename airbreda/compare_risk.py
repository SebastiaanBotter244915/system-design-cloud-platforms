# compare_risk.py — stretch goal: sigmoid-on-regression vs. LogisticRegression on a binary label
from sklearn.linear_model import LogisticRegression

from predict import load_data, load_model, exceedance_risk, NO2_THRESHOLD

X, y = load_data()
exceeded = (y > NO2_THRESHOLD).astype(int)

regression = load_model()
classifier = LogisticRegression().fit(X, exceeded)

out = X.copy()
out["no2_actual"] = y
out["exceeded"] = exceeded
out["no2_pred"] = regression.predict(X).round(2)
out["risk_sigmoid"] = [round(exceedance_risk(p), 3) for p in out["no2_pred"]]
out["risk_logistic"] = classifier.predict_proba(X)[:, 1].round(3)
print(out.to_string(index=False))
