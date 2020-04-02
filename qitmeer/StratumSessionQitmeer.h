/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */

#ifndef STRATUM_SESSION_BITCOIN_H_
#define STRATUM_SESSION_BITCOIN_H_

#include "StratumSession.h"
#include "StratumServerQitmeer.h"
#include "StratumQitmeer.h"

class StratumSessionQitmeer : public StratumSessionBase<StratumTraitsBitcoin> {
public:
  StratumSessionQitmeer(
      ServerBitcoin &server,
      struct bufferevent *bev,
      struct sockaddr *saddr,
      uint32_t extraNonce1);
  uint16_t decodeSessionId(const std::string &exMessage) const override;
  void sendSetDifficulty(LocalJob &localJob, uint64_t difficulty) override;
  void
  sendMiningNotify(shared_ptr<StratumJobEx> exJobPtr, bool isFirstJob) override;

protected:
  void handleRequest(
      const std::string &idStr,
      const std::string &method,
      const JsonNode &jparams,
      const JsonNode &jroot) override;
  void
  handleRequest_MiningConfigure(const string &idStr, const JsonNode &jparams);
  void
  handleRequest_Subscribe(const std::string &idStr, const JsonNode &jparams);
  void
  handleRequest_Authorize(const std::string &idStr, const JsonNode &jparams);
  // request from BTCAgent
  void handleRequest_AgentGetCapabilities(
      const string &idStr, const JsonNode &jparams);
  void handleRequest_SuggestTarget(
      const std::string &idStr, const JsonNode &jparams);

  void logAuthorizeResult(bool success, const string &password) override;

  std::unique_ptr<StratumMessageDispatcher> createDispatcher() override;

public:
  std::unique_ptr<StratumMiner> createMiner(
      const std::string &clientAgent,
      const std::string &workerName,
      int64_t workerId) override;

  void responseError(const string &idStr, int errCode) override;

private:
  uint8_t allocShortJobId();

  uint8_t shortJobIdIdx_;

  uint32_t versionMask_; // version mask that the miner wants
  uint64_t suggestedMinDiff_; // min difficulty that the miner wants
  uint64_t suggestedDiff_; // difficulty that the miner wants
};

#endif // #ifndef STRATUM_SESSION_BITCOIN_H_
