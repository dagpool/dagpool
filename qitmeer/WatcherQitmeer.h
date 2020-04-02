/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#ifndef POOL_WATCHER_BITCOIN_H_
#define POOL_WATCHER_BITCOIN_H_

#include "Watcher.h"
#include "StratumQitmeer.h"

///////////////////////////////// ClientContainer //////////////////////////////
class ClientContainerBitcoin : public ClientContainer {
  bool disableChecking_;
  KafkaSimpleConsumer kafkaStratumJobConsumer_; // consume topic: 'StratumJob'
  thread threadStratumJobConsume_;

  boost::shared_mutex stratumJobMutex_;
  shared_ptr<StratumJobBitcoin>
      poolStratumJob_; // the last stratum job from the pool itself

protected:
  bool initInternal() override;
  void runThreadStratumJobConsume();
  void consumeStratumJob(rd_kafka_message_t *rkmessage);
  void handleNewStratumJob(const string &str);

  PoolWatchClient *
  createPoolWatchClient(const libconfig::Setting &config) override;

public:
  ClientContainerBitcoin(const libconfig::Config &config);
  ~ClientContainerBitcoin();

  bool sendEmptyGBT(
      const string &poolName,
      int32_t blockHeight,
      uint32_t nBits,
      const string &blockPrevHash,
      uint32_t blockTime,
      uint32_t blockVersion);

  const shared_ptr<StratumJobBitcoin> getPoolStratumJob();
  boost::shared_lock<boost::shared_mutex> getPoolStratumJobReadLock();
  bool disableChecking() { return disableChecking_; }
};

///////////////////////////////// PoolWatchClient //////////////////////////////
class PoolWatchClientBitcoin : public PoolWatchClient {
  uint32_t extraNonce1_;
  uint32_t extraNonce2Size_;

  string lastPrevBlockHash_;
  bool disableChecking_;

  void handleStratumMessage(const string &line) override;

public:
  PoolWatchClientBitcoin(
      struct event_base *base,
      ClientContainerBitcoin *container,
      const libconfig::Setting &config);
  ~PoolWatchClientBitcoin();

  void onConnected() override;

  ClientContainerBitcoin *GetContainerBitcoin() {
    return static_cast<ClientContainerBitcoin *>(container_);
  }
};

#endif
