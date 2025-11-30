# app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response, Depends
from sqlalchemy.ext.asyncio import AsyncSession 
from app.core.database import get_db 
from app.core.init_db import init_tables
from app.services.ibge.orchestrator import IbgeEtlOrchestrator
from app.repositories.census_repository import CensusRepository
from app.schemas.geo import FeatureCollection

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_tables()
    yield

app = FastAPI(title="Atibaia Geo-Insights", lifespan=lifespan)

@app.get("/")
async def health_check():
    return {"status": "ok", "message": "Geo-Insights API is running"}

# Rota Antiga (Apenas visualiza JSON)
@app.get("/etl/preview")
async def preview_etl():
    orchestrator = IbgeEtlOrchestrator()
    geojson_data = await orchestrator.get_consolidated_data_json()
    return Response(content=geojson_data, media_type="application/json")

# NOVA ROTA: Executa o ETL e Salva no Banco
@app.post("/etl/sync")
async def sync_etl(db: AsyncSession = Depends(get_db)):
    """
    Dispara o processo de Extração e Carga no Banco de Dados.
    """
    orchestrator = IbgeEtlOrchestrator(db=db)
    result = await orchestrator.sync_database()
    return result

# NOVA ROTA: Consome do Banco de Dados
@app.get("/map", response_model=FeatureCollection)
async def get_map_data(db: AsyncSession = Depends(get_db)):
    """
    Endpoint de Consumo (Frontend).
    Retorna o GeoJSON persistido no Banco de Dados.
    """
    repo = CensusRepository(db)
    features = await repo.get_all_features()
    
    return {"type": "FeatureCollection", "features": features}