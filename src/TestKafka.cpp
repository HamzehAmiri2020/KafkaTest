#include <iostream>
#include "rdkafkacpp.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

using namespace rapidjson;

std::string getJson();

int producer_1() {
	std::string errstr;
	std::string topic_str = "QMalware";

	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	conf->set("metadata.broker.list", "127.0.0.1", errstr);

	RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
	if (!producer) {
		std::cerr << "Failed to create producer: " << errstr << std::endl;
		exit(1);
	}

	std::cout << "% Created producer " << producer->name() << std::endl;

	RdKafka::Topic *topic = NULL;
	if (!topic_str.empty()) {
		topic = RdKafka::Topic::create(producer, topic_str, tconf, errstr);
		if (!topic) {
			std::cerr << "Failed to create topic: " << errstr << std::endl;
			exit(1);
		}
	}
	std::string json = getJson();
	char *jsonFinal = new char[json.length() + 1];
	std::copy(json.begin(), json.end(), jsonFinal);
	RdKafka::ErrorCode resp = producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, jsonFinal, 11, NULL, NULL);

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
	producer_1();
}

