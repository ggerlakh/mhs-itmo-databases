import json
from kafka import KafkaConsumer, TopicPartition
consumer = KafkaConsumer('hw4_taxi', 
                         bootstrap_servers='localhost:29092', 
                         group_id='hw4_taxi_group', 
                        #  enable_auto_commit=False, 
                         auto_offset_reset='earliest'
                         )
# consumer = KafkaConsumer('hw4_taxi', bootstrap_servers='localhost:29092')
# print(f"len consumer = {consumer.end_offsets([TopicPartition('hw4_taxi', 0)])}")
for msg in consumer:
    print(msg)
    print(type(msg.value))
    print(json.loads(msg.value.decode('utf-8')))


# from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

# # Настройки консюмера
# consumer = KafkaConsumer(
#     'test_hw4_taxi',  # Название топика
#     bootstrap_servers='localhost:29092',  # Адрес Kafka брокера
#     group_id='hw4_taxi_group',  # ID группы консюмеров
#     enable_auto_commit=True,  # Отключение автоматического коммита
#     auto_offset_reset='latest'  # Начало с самой ранней доступной записи
# )

# # consumer.assign([TopicPartition('hw4_taxi', 0)])
# print(f"before commit")

# # consumer.commit({
# #      TopicPartition('hw4_taxi', 0): OffsetAndMetadata(0, '')
# # })

# print("commited")

# Цикл для чтения сообщений
# try:
#     while True:
#         # Получение сообщений из топика
#         messages = consumer.poll(timeout_ms=1000)  # Установка таймаута для poll
#         print(f"messages = {messages}")
#         for topic_partition, message_list in messages.items():
#             for message in message_list:
#                 print(f"Received message: {message.value.decode('utf-8')}")
#                 # Здесь можно добавить обработку сообщения
# # Закрытие консюмера
# finally:
#     consumer.close()