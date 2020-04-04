/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#ifndef BLOCK_MAKER_BITCOIN_H_
#define BLOCK_MAKER_BITCOIN_H_

#include "BlockMaker.h"
#include "StratumQitmeer.h"

#include <uint256.h>
#include <primitives/transaction.h>

#include <deque>
#include <boost/date_time/posix_time/posix_time.hpp>

class CBlockHeader;

namespace bpt = boost::posix_time;


////////////////////////////////// BlockMaker //////////////////////////////////
class BlockMakerQitmeer : public BlockMaker {
protected:
  mutex rawGbtLock_;
  size_t kMaxRawGbtNum_; // how many rawgbt should we keep
  // key: gbthash
  std::deque<uint256> rawGbtQ_;
  // key: gbthash, value: block template json
  std::map<uint256, shared_ptr<vector<CTransactionRef>>> rawGbtMap_;

  mutex jobIdMapLock_;
  size_t kMaxStratumJobNum_;
  // key: jobId, value: gbthash
  std::map<uint64_t, uint256> jobId2GbtHash_;

  bpt::ptime lastSubmittedBlockTime;
  uint32_t submittedRskBlocks;

  KafkaSimpleConsumer kafkaConsumerRawGbt_;
  KafkaSimpleConsumer kafkaConsumerStratumJob_;


  void insertRawGbt(
      const uint256 &gbtHash, shared_ptr<vector<CTransactionRef>> vtxs);

  thread threadConsumeRawGbt_;
  thread threadConsumeStratumJob_;


  void runThreadConsumeRawGbt();
  void runThreadConsumeStratumJob();


  void consumeRawGbt(rd_kafka_message_t *rkmessage);
  void consumeStratumJob(rd_kafka_message_t *rkmessage);
  void consumeSolvedShare(rd_kafka_message_t *rkmessage);
  void processSolvedShare(rd_kafka_message_t *rkmessage) override;

  void addRawgbt(const char *str, size_t len);

  void saveBlockToDBNonBlocking(
      const FoundBlock &foundBlock,
      const CBlockHeader &header,
      const uint64_t coinbaseValue,
      const int32_t blksize);
  void _saveBlockToDBThread(
      const FoundBlock &foundBlock,
      const CBlockHeader &header,
      const uint64_t coinbaseValue,
      const int32_t blksize);


  void submitBlockNonBlocking(const string &blockHex);
  void _submitBlockThread(
      const string &rpcAddress,
      const string &rpcUserpass,
      const string &blockHex);
      bool check();

  // read-only definition
  inline shared_ptr<const BlockMakerDefinitionQitmeer> def() {
    return std::dynamic_pointer_cast<const BlockMakerDefinitionQitmeer>(def_);
  }

public:
  BlockMakerQitmeer(
      shared_ptr<BlockMakerDefinition> def,
      const char *kafkaBrokers,
      const MysqlConnectInfo &poolDB);
  virtual ~BlockMakerQitmeer();

  bool init() override;
  void run() override;
};

#endif
