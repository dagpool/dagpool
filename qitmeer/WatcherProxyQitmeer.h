/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#pragma once

#include "Watcher.h"
#include "StratumQitmeer.h"
#include "ShareLogger.h"
#include "MySQLConnection.h"

#include <set>
#include <queue>
#include <atomic>
#include <memory>

class PoolWatchClientBitcoinProxy;

///////////////////////////////// ClientContainer //////////////////////////////
class ClientContainerBitcoinProxy : public ClientContainer {
protected:
  struct JobCache {
    string upstreamJobId_;
    StratumJobBitcoin sJob_;
    size_t clientId_ = 0;
  };

  const time_t kFlushDiskInterval = 10;
  time_t lastFlushTime_ = 0;
  std::unique_ptr<ShareLogWriterBase<ShareBitcoin>> shareLogWriter;

  KafkaSimpleConsumer kafkaSolvedShareConsumer_; // consume solved_share_topic
  thread threadSolvedShareConsume_;

  const size_t kMaxJobCacheSize_ = 256;
  map<uint64_t, JobCache> jobCacheMap_;
  set<string> jobGbtHashSet_;
  std::mutex jobCacheLock_;

  // If the high priority pool (the first pool in the configuration file)
  // does not update the job within the following seconds, accept the job
  // from the low priority pools.
  int poolInactiveInterval = 180;

  time_t lastJobTime_ = 0;
  size_t lastJobClient_ = 0;

  PoolWatchClient *
  createPoolWatchClient(const libconfig::Setting &config) override;
  bool initInternal() override;
  void runThreadSolvedShareConsume();
  void consumeSolvedShare(rd_kafka_message_t *rkmessage);
  void tryFlushSolvedShares();

public:
  ClientContainerBitcoinProxy(const libconfig::Config &config);
  ~ClientContainerBitcoinProxy();

  bool sendJobToKafka(
      const string upstreamJobId,
      const StratumJobBitcoin &job,
      PoolWatchClientBitcoinProxy *client);
};

///////////////////////////////// PoolWatchClient //////////////////////////////
class PoolWatchClientBitcoinProxy : public PoolWatchClient {
protected:
  std::mutex wantSubmittedSharesLock_;
  string wantSubmittedShares_;
  string passwd_;
  string extraNonce1_;
  uint32_t extraNonce2Size_ = 0;
  std::atomic<uint64_t> currentDifficulty_;

  void handleStratumMessage(const string &line) override;

public:
  PoolWatchClientBitcoinProxy(
      struct event_base *base,
      ClientContainerBitcoinProxy *container,
      const libconfig::Setting &config);
  ~PoolWatchClientBitcoinProxy();

  void onConnected() override;
  void submitShare(string submitJson);

  ClientContainerBitcoinProxy *GetContainerBitcoinProxy() {
    return static_cast<ClientContainerBitcoinProxy *>(container_);
  }
};
