from predict import load_model, predict

def test_model_pkl_loads():
    model = load_model()
    assert hasattr(model, "predict")

def test_predict_returns_plausible_values():
    result = predict(total_intensity_veh_per_hr=2500, hour_of_day=18)
    no2 = result["no2_ug_m3_predicted"]
    risk = result["no2_exceedance_risk"]
    assert isinstance(no2, float)
    assert 0 <= no2 <= 200
    assert 0 <= risk <= 1
