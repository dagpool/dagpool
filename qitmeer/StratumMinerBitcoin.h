/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#ifndef STRATUM_MINER_BITCOIN_H_
#define STRATUM_MINER_BITCOIN_H_

#include "StratumServerQitmeer.h"
#include "StratumQitmeer.h"
#include "StratumMiner.h"

class StratumMinerBitcoin : public StratumMinerBase<StratumTraitsBitcoin> {
public:
  static const size_t kMinExtraNonce2Size_ = 4;
  static const size_t kMaxExtraNonce2Size_ = 8;

  StratumMinerBitcoin(
      StratumSessionQitmeer &session,
      const DiffController &diffController,
      const std::string &clientAgent,
      const std::string &workerName,
      int64_t workerId);

  void handleRequest(
      const std::string &idStr,
      const std::string &method,
      const JsonNode &jparams,
      const JsonNode &jroot) override;
  void handleExMessage(const std::string &exMessage) override;
  bool handleCheckedShare(
      const std::string &idStr, size_t chainId, const ShareBitcoin &share);

private:
  void handleRequest_Submit(const std::string &idStr, const JsonNode &jparams);
  void handleExMessage_SubmitShare(
      const std::string &exMessage,
      const bool isWithTime,
      const bool isWithVersion);
  void handleRequest_Submit(
      const std::string &idStr,
      uint8_t shortJobId,
      uint64_t extraNonce2,
      BitcoinNonceType nonce,
      uint32_t nTime,
      uint32_t versionMask);
};

#endif // #ifndef STRATUM_MINER_BITCOIN_H_
