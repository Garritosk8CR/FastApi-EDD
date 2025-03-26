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

def build_state(pk: str):
    pks = Event.all_pks()
    all_events = [Event.get(pk) for pk in pks]
    events = [event for event in all_events if event.delivery_id == pk]
    state = {}
    for event in events:
        state = consumers.CONSUMERS[event.type](state, event)
    return state

@app.get("/deliveries/{pk}/status")
async def get_state(pk: str):
    state = redis_conn.get(f'delivery:{pk}')
    if state is not None:
        return json.loads(state)
    state = build_state(pk)
    redis_conn.set(f'delivery:{pk}', json.dumps(state))
    return  state

@app.post("/deliveries/create")
async def create(request: Request):
    body = await request.json()
    delivery = Delivery(budget=body['data']['budget'], notes=body['data']['notes'])
    delivery.save()
    event = Event(delivery_id=delivery.pk, type=body['type'], data=json.dumps(body['data']))
    event.save()
    state = consumers.CONSUMERS[event.type]({}, event)
    redis_conn.set(f'delivery:{delivery.pk}', json.dumps(state))

    return state if state is not None else {}

@app.post("/event")
async def dispatch(request: Request):
    body = await request.json()
    event = Event(delivery_id=body['delivery_id'], type=body['type'], data=json.dumps(body['data']))
    event.save()
    state = await get_state(body['delivery_id'])
    new_state = consumers.CONSUMERS[event.type](state, event)
    redis_conn.set(f'delivery:{body["delivery_id"]}', json.dumps(new_state))

    return new_state if new_state is not None else {}