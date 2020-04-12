/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#ifndef GBT_MAKER_H_
#define GBT_MAKER_H_

#include "Common.h"
#include "Kafka.h"

#include "zmq.hpp"

/////////////////////////////////// GbtMaker ///////////////////////////////////
class GbtMaker {
  atomic<bool> running_;
  mutex lock_;

  std::unique_ptr<zmq::context_t> zmqContext_;
  string zmqBitcoindAddr_;
  uint32_t zmqTimeout_;

  string bitcoindRpcAddr_;
  string bitcoindRpcUserpass_;
  atomic<uint32_t> lastGbtMakeTime_;

  uint32_t kRpcCallInterval_;

  string kafkaBrokers_;
  string kafkaRawGbtTopic_;
  KafkaProducer kafkaProducer_;
  bool isCheckZmq_;

  bool bitcoindRpcGBT(string &resp);
  string makeRawGbtMsg();
  void submitRawGbtMsg(bool checkTime);

  void threadListenBitcoind();

  void kafkaProduceMsg(const void *payload, size_t len);

public:
  GbtMaker(
      const string &zmqBitcoindAddr,
      uint32_t zmqTimeout,
      const string &bitcoindRpcAddr,
      const string &bitcoindRpcUserpass,
      const string &kafkaBrokers,
      const string &kafkaRawGbtTopic,
      uint32_t kRpcCallInterval,
      bool isCheckZmq);
  ~GbtMaker();

  bool init();
  void stop();
  void run();
};
#endif
