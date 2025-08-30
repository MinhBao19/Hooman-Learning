from openai import OpenAI
from pydantic import BaseModel

def callOpenAI(prompt: str) -> str:
    client = OpenAI()

    class ProductExtraction(BaseModel):
        rank1_name: str
        rank1_price: str
        rank1_sustainability_score10: str
        rank1_health_score10: str

        rank2_name: str
        rank2_price: str
        rank2_sustainability_score10: str
        rank2_health_score10: str

        rank3_name: str
        rank3_price: str
        rank3_sustainability_score10: str
        rank3_health_score10: str

    response = client.responses.parse(
        model="gpt-4o-2024-08-06",
        input=[
            {
                "role": "system",
                "content": "You are an datasorting algorithm returning the highest scoring product from a user specified source. The source will be in the form of a supplier name. You will recieve a range of user inputs which you will use to find and rank products actively sold by this supplier and format it into an output matching the datastructure given to you",
            },
            {"role": "user", "content": f"{prompt}"},
        ],
        text_format=ProductExtraction,
    )

    products = response.output_parsed
    return products