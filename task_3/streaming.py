import logging
import sys
from uuid import uuid4
from pyflink.common import Types, Time, Duration, Row, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.datastream.window import EventTimeSessionWindows
from pyflink.datastream.functions import ProcessWindowFunction, CoProcessFunction
from pyflink.datastream.state import ListStateDescriptor



class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        return int(event['timestamp']) * 1000  # Конвертация в миллисекунды


class EnrichPurchaseFunction(CoProcessFunction):
    def __init__(self):
        self.purchase_state = None
        self.events_state = None

    def open(self, context):
        # Создаю стейт, в который буду сохранять покупки и события без пары.
        # Например, purchase, которая пришла раньше event-a
        self.purchase_state = context.get_list_state(
            ListStateDescriptor("purchases", Types.ROW_NAMED(
                ['amount', 'user_id', 'page_id', 'timestamp'],
                [Types.INT(), Types.INT(), Types.INT(), Types.INT()])))
        self.events_state = context.get_list_state(
            ListStateDescriptor("events", Types.ROW_NAMED(
                ['event_type', 'user_id', 'page_id', 'timestamp'],
                [Types.STRING(), Types.INT(), Types.INT(), Types.INT()])))

    def process_element1(self, user_event, ctx):
        # событие из user_events_stream
        event_timestamp = user_event['timestamp'] * 1000
        for purchase in self.purchase_state:
            purchase_timestamp = purchase['timestamp'] * 1000
            if abs(event_timestamp - purchase_timestamp) <= 5000:  # ±5 секунд
                yield Row(
                    user_id=user_event['user_id'],
                    event_type=user_event['event_type'],
                    page_id=user_event['page_id'],
                    timestamp=user_event['timestamp'],
                    amount=purchase['amount'])
                break
        else:
            if user_event['event_type'] == 'purchase':
                self.events_state.add(user_event)
            yield Row(
                user_id=user_event['user_id'],
                event_type=user_event['event_type'],
                page_id=user_event['page_id'],
                timestamp=user_event['timestamp'],
                amount=0)

    def process_element2(self, purchase, ctx):
        # событие из purchases
        purchase_timestamp = purchase['timestamp'] * 1000
        for event in self.events_state:
            event_timestamp = event['timestamp'] * 1000
            if abs(purchase_timestamp - event_timestamp) <= 5000:
                yield Row(
                    user_id=event['user_id'],
                    event_type=event['event_type'],
                    page_id=event['page_id'],
                    timestamp=event['timestamp'],
                    amount=purchase['amount'])
                break
        else:
            self.purchase_state.add(purchase)

    def on_timer(self, timestamp, ctx):
        # для управления размером состояния
        current_watermark = ctx.get_current_watermark()
        updated_purchases = [
            p for p in self.purchase_state
            if p['timestamp'] * 1000 >= current_watermark - 5000]
        updated_events = [
            p for p in self.updated_events
            if p['timestamp'] * 1000 >= current_watermark - 5000]        
        self.purchase_state.clear()
        self.updated_events.clear()
        self.purchase_state.update(updated_purchases)
        self.updated_events.update(updated_events)        


class SessionGenerator(ProcessWindowFunction):
    def process(self, key, context, elements):
        session_id = str(uuid4()).split('-')[0]
        events = list(elements)
        events = sorted(events, key=lambda x: x['timestamp'])
        session_amounts = [x['amount'] for x in events if x['amount'] > 0]
        for element in elements:
            yield Row(
                session_id=session_id,
                user_id=element['user_id'],
                event_type=element['event_type'],
                page_id=element['page_id'],
                amount=session_amounts[-1] if len(session_amounts) else 0,
                timestamp=element['timestamp'],
                type=None)
            
        # проверка на подозрительность
        suspicious_type = None
        if len(events) > 3:
            suspicious_type = 1
        elif not any(e['event_type'] == 'page-view' for e in events):
            suspicious_type = 2
            
        if suspicious_type is not None:
            yield Row(
                session_id=session_id,
                user_id=key,
                event_type=None,
                page_id=None,
                amount=None,
                timestamp=None,
                type=suspicious_type)

def process_user_events(env):
    events_deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW_NAMED(
            ['event_type', 'user_id', 'page_id', 'timestamp'],
            [Types.STRING(), Types.INT(), Types.INT(), Types.INT()])) \
        .build()
    events_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:19092,localhost:19091,localhost:19090") \
        .set_topics("user-events") \
        .set_group_id("flink-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(events_deserialization_schema) \
        .build()
    purchases_deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(Types.ROW_NAMED(
            ['amount', 'user_id', 'page_id', 'timestamp'],
            [Types.INT(), Types.INT(), Types.INT(), Types.INT()])) \
        .build()
    purchases_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:19092,localhost:19091,localhost:19090") \
        .set_topics("purchases") \
        .set_group_id("flink-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(purchases_deserialization_schema) \
        .build()
    
    visits_schema = Types.ROW_NAMED(
        ['session_id', 'user_id', 'event_type', 'page_id', 'amount', 'timestamp'],
        [Types.STRING(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT()])    
    visits_sink_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(visits_schema) \
        .build()    
    visits_sinker = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:19092,localhost:19091,localhost:19090") \
        .set_transactional_id_prefix("visits-tx-") \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
            .set_topic("visits")
            .set_value_serialization_schema(visits_sink_schema)
            .build()) \
        .build()
    
    susp_visits_schema = Types.ROW_NAMED(
        ['session_id', 'user_id', 'type'],
        [Types.STRING(), Types.INT(), Types.INT()])    
    susp_visits_sink_schema = JsonRowSerializationSchema.Builder() \
        .with_type_info(susp_visits_schema) \
        .build()    
    susp_visits_sinker = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:19092,localhost:19091,localhost:19090") \
        .set_transactional_id_prefix("susp-visits-tx-") \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
            .set_topic("suspicious-visits")
            .set_value_serialization_schema(susp_visits_sink_schema)
            .build()) \
        .build()
    
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(EventTimestampAssigner())
    user_events_stream = env.from_source(events_source, watermark_strategy, source_name="user-events")
    purchases_stream = env.from_source(purchases_source, watermark_strategy, source_name="purchases")

    # джойн по user_id с задержкой ±5 секунд
    enriched_schema = Types.ROW_NAMED(
        ['user_id', 'event_type', 'page_id', 'timestamp', 'amount'],
        [Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT()])    
    enriched_stream = user_events_stream \
        .key_by(lambda e: e['user_id'], key_type=Types.INT()) \
        .connect(purchases_stream.key_by(lambda p: p['user_id'], key_type=Types.INT())) \
        .process(EnrichPurchaseFunction(), output_type=enriched_schema)

    unified_schema = Types.ROW_NAMED(
        ['session_id', 'user_id', 'event_type', 'page_id', 'amount', 'timestamp', 'type'],
        [Types.STRING(), Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.INT(), Types.INT()])
    session_visits = enriched_stream \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda e: e['user_id'], key_type=Types.INT()) \
        .window(EventTimeSessionWindows.with_gap(Time.seconds(20))) \
        .process(SessionGenerator(), output_type=unified_schema)
    
    session_visits \
        .filter(lambda r: r['type'] is None) \
        .map(lambda r: Row(
                session_id=r['session_id'], user_id=r['user_id'], 
                event_type=r['event_type'], page_id=r['page_id'], 
                amount=r['amount'], timestamp=r['timestamp']), 
             output_type=visits_schema) \
        .sink_to(visits_sinker)
    
    session_visits \
        .filter(lambda r: r['type'] is not None) \
        .map(lambda r: Row(
                session_id=r['session_id'], user_id=r['user_id'], type=r['type']), 
             output_type=susp_visits_schema) \
        .sink_to(susp_visits_sinker)

    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///home/timosha/flink/lib/flink-sql-connector-kafka-4.0.0-2.0.jar")
    env.set_parallelism(1)
    try:
        process_user_events(env)
    except KeyboardInterrupt:
        pass
