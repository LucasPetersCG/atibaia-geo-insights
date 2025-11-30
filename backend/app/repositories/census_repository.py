# app/repositories/census_repository.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, func, text
from sqlalchemy.dialects.postgresql import insert
from app.models.census import CensusTract
import geopandas as gpd
import logging
import json

logger = logging.getLogger(__name__)

class CensusRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def save_tracts(self, gdf: gpd.GeoDataFrame):
        """
        Recebe um GeoDataFrame e salva no Banco de Dados.
        Usa 'Upsert' (Update on Conflict) ou limpa e insere tudo.
        Para simplificar nesta fase, vamos limpar e inserir (Full Refresh).
        """
        if gdf.empty:
            logger.warning("Tentativa de salvar GeoDataFrame vazio.")
            return

        logger.info(f"💾 Salvando {len(gdf)} setores no PostGIS...")

        # 1. Limpar tabela atual (Estratégia Full Refresh)
        # Em produção, faríamos um Update incremental, mas para MVP isso garante consistência.
        await self.db.execute(delete(CensusTract))
        
        # 2. Converter GeoDataFrame para Lista de Dicionários compatíveis com o Modelo
        # Precisamos garantir que a geometria esteja em WKT (Texto) para o SQLAlchemy entender
        tracts_to_insert = []
        
        for _, row in gdf.iterrows():
            tracts_to_insert.append({
                "code": str(row["code"]),
                "population": int(row["population"]),
                # GeoAlchemy2 aceita WKT (Well-Known Text) direto na string
                "geom": row["geometry"].wkt 
            })

        # 3. Bulk Insert (Muito mais rápido que inserir um por um)
        try:
            await self.db.execute(
                insert(CensusTract),
                tracts_to_insert
            )
            await self.db.commit()
            logger.info("✅ Dados persistidos com sucesso!")
            
        except Exception as e:
            logger.error(f"Erro ao salvar no banco: {e}")
            await self.db.rollback()
            raise e

    async def get_all_tracts(self):
        """Busca todos os setores para exibir na API."""
        result = await self.db.execute(select(CensusTract))
        return result.scalars().all()

    async def get_all_features(self):
        """
        Retorna todos os setores formatados como GeoJSON.
        Usa ST_AsGeoJSON do PostGIS para máxima performance.
        """
        stmt = select(
            CensusTract.code,
            CensusTract.population,
            func.ST_AsGeoJSON(CensusTract.geom).label("geometry_json")
        )
        
        result = await self.db.execute(stmt)
        rows = result.all()
        
        features = []
        for row in rows:
            features.append({
                "type": "Feature",
                "geometry": json.loads(row.geometry_json), # Converte string JSON do banco para dict
                "properties": {
                    "code": row.code,
                    "population": row.population
                }
            })
            
        return features