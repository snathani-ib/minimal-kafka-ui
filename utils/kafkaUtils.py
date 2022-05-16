from kafka import KafkaConsumer, cluster, KafkaAdminClient
import sys

# Define server with port
#bootstrap_servers = ['localhost:9092']

class kafkaUtils:
	def __init__(self, bootstrap_servers):
		self.bootstrap_servers = bootstrap_servers
		self.cluster_admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
		self.cluster_client = cluster.ClusterMetadata(bootstrap_servers=self.bootstrap_servers)
		self.consumer_client = KafkaConsumer(group_id='test-1', bootstrap_servers=self.bootstrap_servers)


	def get_brokers(self):
		broker_details = self.cluster_client.brokers()
		print(broker_details)
		return broker_details

	def broker_metadata(self):
		broker_metadata = self.cluster_client.broker_metadata()
		print(broker_metadata)

	def get_kafka_topics(self):
		kafka_topics = self.consumer_client.topics()
		print(kafka_topics)
		if kafka_topics:
			return str(kafka_topics)
		else:
			return str(())

	def get_messages_all_topics(self):
		list_kafka_topics = kafka_topics = self.consumer_client.topics()
		# print(type(list_kafka_topics))
		for topic_name in list_kafka_topics:
			# topic_name = "metrics-rdbms-7"
			#partition_details = self.cluster_client.partitions_for_topic(topic_name)
			self.get_messages_from_topic(topic_name)

	def get_messages_from_topic(self,topic_name):
		consumer = KafkaConsumer(topic_name, group_id=None,bootstrap_servers=self.bootstrap_servers,consumer_timeout_ms=1000,auto_offset_reset='earliest',enable_auto_commit=True)
		list_messages = []

		print("Here")
		# Read and print message from consumer
		# print(type(consumer))
		for msg in consumer:
			print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))
			print(msg.value)
			list_messages.append(msg.value)

		return list_messages


	def partitions_for_topic(self):
		import json
		list_kafka_topics = kafka_topics = self.consumer_client.topics()
		# print(type(list_kafka_topics))
		for topic_name in list_kafka_topics:
			# topic_name = "metrics-rdbms-7"
			#partition_details = self.cluster_client.partitions_for_topic(topic_name)
			partition_details = self.consumer_client.partitions_for_topic(topic_name)
			print(topic_name + ": " + str(list(partition_details)))
			# for partition in partition_details:
			# 	print(partition)

	def list_consumer_groups(self):
		print(self.cluster_admin_client.list_consumer_groups())

	def describe_consumer_groups(self):
		print(self.cluster_admin_client.describe_consumer_groups())

	def list_consumer_group_offsets(self):
		print(self.cluster_admin_client.list_consumer_group_offsets())

	def list_consumer_group_offsets(self):
		print(self.cluster_admin_client.describe_configs(None))

	def offsets_for_topic(self):
		#Get partitions for topic
		#Get offsets for each partition
		#Present them
		pass

	def beginning_offsets_all_topics(self):
		list_kafka_topics = kafka_topics = self.consumer_client.topics()
		# print(type(list_kafka_topics))
		for topic_name in list_kafka_topics:
			partition_details = self.consumer_client.partitions_for_topic(topic_name)
			list_tp = []
			for partition in partition_details:
				tp1 = TopicPartition(topic_name, partition)
				list_tp.append(tp1)
			self.beginning_offsets(list_tp)

	def beginning_offsets(self, list_partition):
		offsets = self.consumer_client.beginning_offsets(list_partition)
		# print(str(list_partition))
		print(offsets)

	def end_offsets_all_topics(self):
		list_kafka_topics = kafka_topics = self.consumer_client.topics()
		# print(type(list_kafka_topics))
		for topic_name in list_kafka_topics:
			partition_details = self.consumer_client.partitions_for_topic(topic_name)
			list_tp = []
			for partition in partition_details:
				tp1 = TopicPartition(topic_name, partition)
				list_tp.append(tp1)
			self.end_offsets(list_tp)


	def end_offsets(self, list_partition):
		offsets = self.consumer_client.end_offsets(list_partition)
		print(offsets)


from kafka import TopicPartition
#obj_kafkaUtils = kafkaUtils(bootstrap_servers="localhost:9092")
obj_kafkaUtils = kafkaUtils(bootstrap_servers=["ib-kafka-0:9093"])

print("Brokers ----------------------")
obj_kafkaUtils.get_brokers()

print("Brokers Metadata ----------------------")
obj_kafkaUtils.broker_metadata()

# print("List topics ----------------------")
# obj_kafkaUtils.get_kafka_topics()

print("List topics ----------------------")
obj_kafkaUtils.partitions_for_topic()

print("Beginning offset ----------------------")
obj_kafkaUtils.beginning_offsets_all_topics()

print("End offset ----------------------")
obj_kafkaUtils.end_offsets_all_topics()

sys.exit()

print("List messages all topics ----------------------")
#obj_kafkaUtils.get_messages_all_topics()
#obj_kafkaUtils.get_messages_from_topic("metrics-rdbms-7")

print("List consumer groups ----------------------")
obj_kafkaUtils.list_consumer_groups()

#bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "metrics-rdbms-7" --from-beginning