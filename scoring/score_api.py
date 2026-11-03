"""
CNC Guard — Scoring API
FastAPI mikrozerbitzu bat: sentsore-irakurketa bat jasotzen du eta
entrenatutako ereduaren bidez hutsegite-probabilitatea itzultzen du.

Helburua: NiFi-k InvokeHTTP bidez deitzen dio fase 3an.
"""

import os
import logging
import time
from contextlib import asynccontextmanager

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Depends, Security, status
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SCORING] %(message)s")
log = logging.getLogger(__name__)

MODEL_PATH = os.getenv("MODEL_PATH", "../models/cnc_failure_model.pkl")
API_KEY_SECRET = os.getenv("API_KEY_SECRET", "super-secret-key-change-in-prod")
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def check_api_key(api_key: str = Security(api_key_header)):
    if not api_key or api_key != API_KEY_SECRET:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Baimenik gabeko sarbidea."
        )
    return api_key

CORS_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:3000,http://localhost:5173",
    ).split(",")
    if origin.strip()
]

# Modeloa aplikazio-abieran behin kargatzen da (ez eskaera bakoitzean)
model = None

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

# Mugatu gabeko jatorrietara mugatzen da CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
    expose_headers=["X-Process-Time"],
)

@app.middleware("http")
async def prozesatzeko_denbora(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Process-Time"] = str(time.perf_counter() - start)
    return response


# ── Eskema ────────────────────────────────────────────────────────────────────


class SentsoreSaria(BaseModel):
    timestamp: str = Field(..., max_length=50)
    machine_id: str = Field(..., pattern="^CNC-[0-9]{3}$")
    machine_type: str = Field(..., pattern="^[LMH]$")
    air_temp_K: float = Field(..., ge=200.0, le=400.0)
    process_temp_K: float = Field(..., ge=200.0, le=400.0)
    rpm: int = Field(..., ge=0, le=10000)
    torque_Nm: float = Field(..., ge=0.0, le=500.0)
    tool_wear_min: int = Field(..., ge=0, le=1000)


class Predikzioa(BaseModel):
    machine_id: str
    failure_predicted: bool
    failure_probability: float
    alert_level: str  # "OK" | "KONTUZ" | "LARRIA"


# ── Endpointak ────────────────────────────────────────────────────────────────


@app.get("/health")
def health(api_key: str = Depends(check_api_key)):
    return {"status": "ok", "model_loaded": model is not None, "model_path": MODEL_PATH}

@app.post("/predict", response_model=Predikzioa)
def predict(saria: SentsoreSaria, api_key: str = Depends(check_api_key)):
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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail="Errorea barne prozesamenduan."
        )
