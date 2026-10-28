"""
CNC Guard — Scoring API
FastAPI mikrozerbitzu bat: sentsore-irakurketa bat jasotzen du eta
entrenatutako ereduaren bidez hutsegite-probabilitatea itzultzen du.

Helburua: NiFi-k InvokeHTTP bidez deitzen dio fase 3an.
"""

import os
import logging
from contextlib import asynccontextmanager

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SCORING] %(message)s")
log = logging.getLogger(__name__)

MODEL_PATH = os.getenv("MODEL_PATH", "../models/cnc_failure_model.pkl")

# Modeloa aplikazio-abieran behin kargatzen da (ez eskaera bakoitzean)
model = joblib.load(MODEL_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global model

    log.info("Modeloa kargatzen...")
    if os.path.exists(MODEL_PATH):
        try:
            model = joblib.load(MODEL_PATH)
            log.info("Modeloa kargatuta.")
        except Exception as e:
            log.exception(f"Errorea modeloa kargatzean: {e}")
            model = None
    else:
        log.warning(f"Ez da aurkitu modeloa bide honetan: {MODEL_PATH}.")

    yield


app = FastAPI(
    title="CNC Guard Scoring API",
    description="Predictive maintenance — hutsegite-probabilitate prediktorea",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Eskema ────────────────────────────────────────────────────────────────────


class SentsoreSaria(BaseModel):
    timestamp: str
    machine_id: str
    machine_type: str = Field(pattern="^[LMH]$")
    air_temp_K: float
    process_temp_K: float
    rpm: int
    torque_Nm: float
    tool_wear_min: int


class Predikzioa(BaseModel):
    machine_id: str
    failure_predicted: bool
    failure_probability: float
    alert_level: str  # "OK" | "KONTUZ" | "LARRIA"


# ── Endpointak ────────────────────────────────────────────────────────────────


@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None, "model_path": MODEL_PATH}


@app.post("/predict", response_model=Predikzioa)
def predict(saria: SentsoreSaria):
    try:
        if model is None:
            raise HTTPException(status_code=503, detail="Eredua ez dago kargatuta.")

        # Sarrera dataframe baten bihurtu
        input_data = pd.DataFrame(
            [
                {
                    "air_temp_K": saria.air_temp_K,
                    "process_temp_K": saria.process_temp_K,
                    "rpm": saria.rpm,
                    "torque_Nm": saria.torque_Nm,
                    "tool_wear_min": saria.tool_wear_min,
                }
            ]
        )

        # Feature engineering: ezaugarri berriak kalkulatu.
        input_data["power"] = input_data["rpm"] * input_data["torque_Nm"]

        input_data["temp_diff"] = (
            input_data["process_temp_K"] - input_data["air_temp_K"]
        )

        input_data["torque_wear"] = (
            input_data["torque_Nm"] * input_data["tool_wear_min"]
        )

        # machine_type aldagaiaren one-hot kodifikazioa.
        input_data["machine_type_L"] = int(saria.machine_type == "L")
        input_data["machine_type_M"] = int(saria.machine_type == "M")

        # Entrenamenduan erabilitako ezaugarrien ordena mantendu.
        if hasattr(model, "feature_names_in_"):
            expected_features = list(model.feature_names_in_)

            missing_features = [
                feature
                for feature in expected_features
                if feature not in input_data.columns
            ]

            if missing_features:
                raise HTTPException(
                    status_code=500,
                    detail="Entrenamenduko ezaugarriak falta dira: "
                    + ", ".join(missing_features),
                )

            input_data = input_data[expected_features]

        # Interferentzia egin
        # Ereduarekin hutsegite-probabilitatea kalkulatu.
        probabilities = model.predict_proba(input_data)[0]

        classes = list(model.classes_)
        failure_index = classes.index(1)

        prob = float(probabilities[failure_index])

        # 0.40ko atalasea aplikatu.
        failure_predicted = prob >= 0.40

        # Alert maila zehaztu
        if prob < 0.40:
            alert_level = "OK"
        elif prob <= 0.80:
            alert_level = "KONTUZ"
        else:
            alert_level = "LARRIA"

        return Predikzioa(
            machine_id=saria.machine_id,
            failure_predicted=failure_predicted,
            failure_probability=round(prob, 4),
            alert_level=alert_level,
        )

    except HTTPException:
        raise

    except Exception as e:
        log.exception(f"Errorea predikzioa egitean: {e}")
        raise HTTPException(status_code=500, detail=f"Errorea inferentzian: {str(e)}")
