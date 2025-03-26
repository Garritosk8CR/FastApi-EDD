import json
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel


import consumers


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

redis_conn = get_redis_connection(
    host="redis-12894.c1.us-east1-2.gce.redns.redis-cloud.com",
    port=12894,
    password="oUDjGzd2h2ljd6FyVIMdyvTp4gE3celk",
    decode_responses=True
)

class Delivery(HashModel):
    budget: int = 0
    notes: str = ''

    class Meta:
        database = redis_conn

class Event(HashModel):
    delivery_id: str = None
    type: str
    data: str

    class Meta:
        database = redis_conn

@app.get("/deliveries/{pk}/status")
async def get_deliveries():
    state = redis_conn.get(f'delivery:{pk}')
    return json.loads(state) if state is not None else {}
@app.post("/deliveries/create")
async def create(request: Request):
    body = await request.json()
    delivery = Delivery(budget=body['data']['budget'], notes=body['data']['notes'])
    delivery.save()
    event = Event(delivery_id=delivery.pk, type=body['type'], data=json.dumps(body['data']))
    event.save()
    state = consumers.create_delivery({}, event)
    redis_conn.set(f'delivery:{delivery.pk}', json.dumps(state))

    return state if state is not None else {}
