import os
from pathlib import Path
from typing import Optional, Dict, Any, List

from pydantic import BaseModel
from openai import OpenAI

def Top3Product(prompt: str) -> str:
    client = OpenAI()

    class ProductExtraction(BaseModel):
        rank1_name: str
        rank1_price: str
        rank1_sustainability_score10: str
        rank1_health_score10: str
        rank1_scoreReasoning: str

        rank2_name: str
        rank2_price: str
        rank2_sustainability_score10: str
        rank2_health_score10: str
        rank2_scoreReasoning: str

        rank3_name: str
        rank3_price: str
        rank3_sustainability_score10: str
        rank3_health_score10: str
        rank3_scoreReasoning: str

    response = client.responses.parse(
        model="gpt-4o-2024-08-06",
        input=[
            {
                "role": "system",
                "content": "You are an datasorting algorithm returning the highest scoring product from a user specified source. The source will be in the form of a supplier name. You will recieve a range of user inputs which you will use to find and rank products actively sold by this supplier and format it into an output matching the datastructure given to you. Summaries produced must relate to scores given.",
            },
            {"role": "user", "content": f"{prompt}"},
        ],
        text_format=ProductExtraction,
    )

    products = response.output_parsed
    return products


def ProductSummary(client: OpenAI, row: Dict[str, Any]) -> str:
    payload = {
        "name": row.get("description"),
        "brand_owner": row.get("brand_owner"),
        "category": row.get("fdc_category"),
        "supplier_store": row.get("store"),
        "price_per_unit_aud": row.get("price_per_unit_aud"),
        "ratings": {
            "healthiness": row.get("rating_healthiness"),
            "sustainability": row.get("rating_sustainability"),
        },
    }
    resp = client.responses.parse(
        model="gpt-4o-2024-08-06",
        input=[
            {"role": "system", "content": "Return an informative and objective 20-word summary for Australian shoppers."},
            {"role": "user", "content": str(payload)},
        ],
        text_format=ProductSummary,
    )
    return resp.output_parsed.summary_20_words