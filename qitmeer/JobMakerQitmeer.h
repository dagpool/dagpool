/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#ifndef JOB_MAKER_BITCOIN_H_
#define JOB_MAKER_BITCOIN_H_

#include "JobMaker.h"


#if defined(CHAIN_TYPE_ZEC) && defined(NDEBUG)
// fix "Zcash cannot be compiled without assertions."
#undef NDEBUG
#include <crypto/common.h>
#define NDEBUG
#endif

#include <uint256.h>
#include <base58.h>

class JobMakerHandlerBitcoin : public JobMakerHandler {
  // mining bitcoin blocks
  CTxDestination poolPayoutAddr_;
  uint32_t currBestHeight_;
  uint32_t lastJobSendTime_;
  bool isLastJobEmptyBlock_;
  std::map<uint64_t /* @see makeGbtKey() */, string>
      rawgbtMap_; // sorted gbt by timestamp
  deque<uint256> lastestGbtHash_;

  // merged mining for AuxPow blocks (example: Namecoin, ElastOS)
  string latestNmcAuxBlockJson_;
  string latestNmcAuxBlockHash_;
  uint32_t latestNmcAuxBlockHeight_;

  bool addRawGbt(const string &msg);
  void clearTimeoutGbt();
  bool isReachTimeout();

  void clearTimeoutGw();
  void clearVcashTimeoutGw();

  // return false if there is no best rawGbt or
  // doesn't need to send a stratum job at current.
  bool findBestRawGbt(string &bestRawGbt);
  string makeStratumJob(const string &gbt);

  inline uint64_t
  makeGbtKey(uint32_t gbtTime, bool isEmptyBlock, uint32_t height);
  inline uint32_t gbtKeyGetTime(uint64_t gbtKey);
  inline uint32_t gbtKeyGetHeight(uint64_t gbtKey);
  inline bool gbtKeyIsEmptyBlock(uint64_t gbtKey);

public:
  JobMakerHandlerBitcoin();
  virtual ~JobMakerHandlerBitcoin() {}

  bool init(shared_ptr<JobMakerDefinition> def) override;
  virtual bool initConsumerHandlers(
      const string &kafkaBrokers,
      vector<JobMakerConsumerHandler> &handlers) override;

  bool processRawGbtMsg(const string &msg);
  bool processAuxPowMsg(const string &msg);
  bool processRskGwMsg(const string &msg);
  bool processVcashGwMsg(const string &msg);

  virtual string makeStratumJobMsg() override;

  // read-only definition
  inline shared_ptr<const GbtJobMakerDefinition> def() {
    return std::dynamic_pointer_cast<const GbtJobMakerDefinition>(def_);
  }
};

#endif
