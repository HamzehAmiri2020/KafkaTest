#include <iostream>
#include "rdkafkacpp.h"
#include "rdkafka.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

using namespace rapidjson;

std::string getJson();

void msg_consume(RdKafka::Message *message, void *opaque) {

	switch (message->err()) {
	case RdKafka::ERR__TIMED_OUT:
		break;
	case RdKafka::ERR_NO_ERROR:
		std::cout << "Read msg at offset " << message->offset() << std::endl;
		if (message->key()) {
			std::cout << "Key: " << *message->key() << std::endl;
		}

		printf("%.*s\n", static_cast<int>(message->len()), static_cast<const char*>(message->payload()));
		break;
	case RdKafka::ERR__PARTITION_EOF:
		break;
	case RdKafka::ERR__UNKNOWN_TOPIC:
	case RdKafka::ERR__UNKNOWN_PARTITION:
		break;
	default:
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
	}
}

class ExampleConsumeCb: public RdKafka::ConsumeCb {
public:
	void consume_cb(RdKafka::Message &msg, void *opaque) {
		msg_consume(&msg, opaque);
	}
};

//void consumer_2() {
//	rd_kafka_t *rk;/* Consumer instance handle */
//	rd_kafka_conf_t *conf;/* Temporary configuration object */
//	rd_kafka_resp_err_t err;/* librdkafka API error code */
//	char errstr[512];/* librdkafka API error reporting buffer */
//	const char *brokers = "127.0.0.1";/* Argument: broker list */
//	const char *groupid = "0";/* Argument: Consumer group id */
//	char **topics;/* Argument: list of topics to subscribe to */
//	int topic_cnt;/* Number of topics to subscribe to */
//	rd_kafka_topic_partition_list_t *subscription;/* Subscribed topics */
//	int i;
//
//
//	conf = rd_kafka_conf_new();
//}

void cosumer_1() {
	std::string errstr;
	std::string brokers = "127.0.0.1";
	std::string topic_str = "QMalware";

	int32_t partition = 0/*RdKafka::Topic::PARTITION_UA*/;
	int64_t start_offset = RdKafka::Topic::OFFSET_STORED;

	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	conf->set("metadata.broker.list", brokers, errstr);
	conf->set("bootstrap.servers", brokers, errstr);
	conf->set("auto.offset.reset", "earliest", errstr);
	conf->set("group.id", "0", errstr);
	conf->set("max.poll.records", "20", errstr);
	conf->set("enable.partition.eof", "true", errstr);

	RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
	RdKafka::Topic *topic = RdKafka::Topic::create(consumer, topic_str, tconf, errstr);

	RdKafka::ErrorCode resp = consumer->start(topic, partition, start_offset);
	if (resp != RdKafka::ERR_NO_ERROR) {
		std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp) << std::endl;
		exit(1);
	}

	ExampleConsumeCb ex_consume_cb;

	bool use_ccb = true;

	int run = 1;

	while (run) {
		if (use_ccb) {
			consumer->consume_callback(topic, partition, 1000, &ex_consume_cb, &use_ccb);
		} else {
			RdKafka::Message *msg = consumer->consume(topic, partition, 1000);
			msg_consume(msg, NULL);
			delete msg;
		}
		consumer->poll(0);
	}

	consumer->stop(topic, partition);
	consumer->poll(1000);

	delete topic;
	delete consumer;
}

int producer_1() {
	std::string errstr;
	std::string topic_str = "QMalware";
	int32_t partition = RdKafka::Topic::PARTITION_UA;

	std::string json = getJson();
	char *jsonFinal = new char[json.length() + 1];
	std::copy(json.begin(), json.end(), jsonFinal);

	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	conf->set("metadata.broker.list", "127.0.0.1", errstr);

	RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
	RdKafka::Topic *topic = RdKafka::Topic::create(producer, topic_str, tconf, errstr);

	RdKafka::ErrorCode resp = producer->produce(topic, partition, RdKafka::Producer::RK_MSG_COPY, jsonFinal, json.length(), NULL, NULL);

	if (resp != RdKafka::ERR_NO_ERROR) {
		std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp) << std::endl;
		exit(1);
	}
	delete topic;
	delete producer;
	delete tconf;
	return 0;
}

template<typename T>
T add(T x, T y) {
	return x + y;
}

std::string getJson() {
	char *json = "{\"project\":\"rapidjson\",\"stars\":10}";
	Document d;
	d.Parse(json);

	Value &s = d["stars"];
	s.SetInt(s.GetInt() + 1);

	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	d.Accept(writer);

	char *output_json = const_cast<char*>(buffer.GetString());
	std::string msgJson(output_json);
	std::cout << msgJson << std::endl;
	return msgJson;
}

int main(int argc, char **argv) {
//	std::cout << add(10,10);
//	producer_1();
	cosumer_1();
}

