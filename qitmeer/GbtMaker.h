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
#if defined(CHAIN_TYPE_BCH) || defined(CHAIN_TYPE_BSV)
  atomic<uint32_t> lastGbtLightMakeTime_;
#endif
  uint32_t kRpcCallInterval_;

  string kafkaBrokers_;
  string kafkaRawGbtTopic_;
  KafkaProducer kafkaProducer_;
  bool isCheckZmq_;

  bool bitcoindRpcGBT(string &resp);
  string makeRawGbtMsg();
  void submitRawGbtMsg(bool checkTime);

#if defined(CHAIN_TYPE_BCH) || defined(CHAIN_TYPE_BSV)
  bool bitcoindRpcGBTLight(string &resp);
  string makeRawGbtLightMsg();
  void submitRawGbtLightMsg(bool checkTime);
#endif

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
#if defined(CHAIN_TYPE_BCH) || defined(CHAIN_TYPE_BSV)
  void runLightGbt();
#endif
  void run();
};

//////////////////////////////// NMCAuxBlockMaker //////////////////////////////
//
// rpc call: ./namecoin-cli createauxblock N59bssPo1MbK3khwPELTEomyzYbHLb59uY
//
class NMCAuxBlockMaker {
  atomic<bool> running_;
  mutex lock_;

  std::unique_ptr<zmq::context_t> zmqContext_;
  string zmqNamecoindAddr_;
  uint32_t zmqTimeout_;

  string rpcAddr_;
  string rpcUserpass_;
  atomic<uint32_t> lastCallTime_;
  uint32_t kRpcCallInterval_;
  string fileLastRpcCallTime_;

  string kafkaBrokers_;
  string kafkaAuxPowGwTopic_;
  KafkaProducer kafkaProducer_;
  bool isCheckZmq_;
  string coinbaseAddress_; // nmc coinbase payout address
  bool useCreateAuxBlockInterface_;

  bool callRpcCreateAuxBlock(string &resp);
  string makeAuxBlockMsg();

  void submitAuxblockMsg(bool checkTime);
  void threadListenNamecoind();

  void kafkaProduceMsg(const void *payload, size_t len);

public:
  NMCAuxBlockMaker(
      const string &zmqNamecoindAddr,
      uint32_t zmqTimeout,
      const string &rpcAddr,
      const string &rpcUserpass,
      const string &kafkaBrokers,
      const string &kafkaAuxPowGwTopic,
      uint32_t kRpcCallInterval,
      const string &fileLastRpcCallTime,
      bool isCheckZmq,
      const string &coinbaseAddress);
  ~NMCAuxBlockMaker();

  bool init();
  void stop();
  void run();
};

#endif
