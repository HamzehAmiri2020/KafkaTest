#include <iostream>
#include "rdkafkacpp.h"
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

using namespace rapidjson;

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

	RdKafka::ErrorCode resp = producer->produce(topic, RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY, const_cast<char*>("hello worlf"), 11, NULL, NULL);

	delete topic;
	delete producer;
	delete tconf;
	return 0;
}

int main(int argc, char **argv) {
//	producer_1();
// 1. Parse a JSON string into DOM.
	const char *json = "{\"project\":\"rapidjson\",\"stars\":10}";
	Document d;
	d.Parse(json);

// 2. Modify it by DOM.
	Value &s = d["stars"];
	s.SetInt(s.GetInt() + 1);

// 3. Stringify the DOM
	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	d.Accept(writer);

// Output {"project":"rapidjson","stars":11}
	std::cout << buffer.GetString() << std::endl;
}

