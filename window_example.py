from datetime import datetime, timedelta
from time import time
import random
import faust


class OrderModel(faust.Record):
    date: datetime
    kitchen: str
    items: int


class AggOrderModel(faust.Record):
    date: datetime
    count: int
    kitchen: str


TOPIC = 'event-orders'
SINK = 'event-orders-agg'
TABLE = 'tumbling_table'
KAFKA = 'kafka://localhost:9092'
CLEANUP_INTERVAL = 1.0
WINDOW = 10
WINDOW_EXPIRES = 1
PARTITIONS = 1

app = faust.App('windowed-agg', broker=KAFKA, version=1, topic_partitions=PARTITIONS)

app.conf.table_cleanup_interval = CLEANUP_INTERVAL
source = app.topic(TOPIC, value_type=OrderModel)
sink = app.topic(SINK, value_type=AggOrderModel)


def window_processor(key, events):
    timestamp = key[1][0]
    kitchen = key[0]
    all_items = [event.items for event in events]
    count = len(all_items)

    print(
        f'processing window for kitchen {kitchen}: '
        f'{len(all_items)} events,'
        f'timestamp {timestamp}',
    )

    sink.send_soon(value=AggOrderModel(date=timestamp, kitchen=kitchen, count=count))


tumbling_table = (
    app.Table(
        TABLE,
        default=list,
        partitions=PARTITIONS,
        on_window_close=window_processor,
    )
        .tumbling(WINDOW, expires=timedelta(seconds=WINDOW_EXPIRES))
        .relative_to_field(OrderModel.date)
)


@app.agent(source)
async def print_windowed_events(stream):
    async for order in stream:
        value_list = tumbling_table[order.kitchen].value()
        value_list.append(order)
        tumbling_table[order.kitchen] = value_list


@app.timer(0.5)
async def produce():
    await source.send(value=OrderModel(kitchen=random.choice(['k1', 'k2']), items=random.randint(1, 5), date=int(time())))


if __name__ == '__main__':
    app.main()
