# app/schemas/geo.py
from pydantic import BaseModel
from typing import List, Dict, Any

class FeatureProperties(BaseModel):
    """Atributos não espaciais do setor."""
    code: str
    population: int
    # Futuro: renda, densidade, nome_bairro...

class Feature(BaseModel):
    """Estrutura de uma Feature GeoJSON."""
    type: str = "Feature"
    geometry: Dict[str, Any]  # Onde vai o Polygon/MultiPolygon
    properties: FeatureProperties

class FeatureCollection(BaseModel):
    """Estrutura Raiz do GeoJSON."""
    type: str = "FeatureCollection"
    features: List[Feature]