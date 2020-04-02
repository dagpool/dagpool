/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#include "JobMakerQitmeer.h"
#include "CommonQitmeer.h"
#include "StratumQitmeer.h"
#include "QitmeerUtils.h"

#include <iostream>
#include <stdlib.h>

#include <glog/logging.h>
#include <librdkafka/rdkafka.h>

#include <hash.h>
#include <script/script.h>
#include <uint256.h>
#include <util.h>

#if defined(CHAIN_TYPE_BSV)
bool fRequireStandard = true;
#endif

#include "utilities_js.hpp"
#include "Utils.h"

////////////////////////////////JobMakerHandlerBitcoin//////////////////////////////////
JobMakerHandlerBitcoin::JobMakerHandlerBitcoin()
  : currBestHeight_(0)
  , lastJobSendTime_(0)
  , isLastJobEmptyBlock_(false)
  , latestNmcAuxBlockHeight_(0) {
}

bool JobMakerHandlerBitcoin::init(shared_ptr<JobMakerDefinition> defPtr) {
  JobMakerHandler::init(defPtr);

  // select chain
  if (def()->testnet_) {
    SelectParams(CBaseChainParams::TESTNET);
    LOG(WARNING) << "using bitcoin testnet3";
  } else {
    SelectParams(CBaseChainParams::MAIN);
  }

  LOG(INFO) << "Block Version: " << std::hex << def()->blockVersion_;
  LOG(INFO) << "Coinbase Info: " << def()->coinbaseInfo_;
  LOG(INFO) << "Payout Address: " << def()->payoutAddr_;

  // check pool payout address
  if (!BitcoinUtils::IsValidDestinationString(def()->payoutAddr_)) {
    LOG(ERROR) << "invalid pool payout address";
    return false;
  }
  poolPayoutAddr_ = BitcoinUtils::DecodeDestination(def()->payoutAddr_);

  return true;
}

bool JobMakerHandlerBitcoin::initConsumerHandlers(
    const string &kafkaBrokers, vector<JobMakerConsumerHandler> &handlers) {
  std::vector<std::tuple<std::string, int>> topics{
      std::make_tuple(def()->rawGbtTopic_, 0),
      std::make_tuple(def()->auxPowGwTopic_, 0),
      std::make_tuple(def()->rskRawGwTopic_, 0),
      std::make_tuple(def()->vcashRawGwTopic_, 0)};

  auto kafkaConsumer =
      std::make_shared<KafkaQueueConsumer>(kafkaBrokers, topics);
  std::map<string, string> options{{"fetch.wait.max.ms", "5"}};
  kafkaConsumer->setup(1, &options);
  JobMakerConsumerHandler handler{
      kafkaConsumer, [this](const std::string &msg, const std::string &topic) {
        if (topic == def()->rawGbtTopic_) {
          return processRawGbtMsg(msg);
        } else if (topic == def()->auxPowGwTopic_) {
          return processAuxPowMsg(msg);
        } else if (topic == def()->rskRawGwTopic_) {
          return processRskGwMsg(msg);
        } else if (topic == def()->vcashRawGwTopic_) {
          return processVcashGwMsg(msg);
        }
        return false;
      }};
  handlers.push_back(handler);

  // sleep 3 seconds, wait for the latest N messages transfer from broker to
  // client
  std::this_thread::sleep_for(3s);

  /* pre-consume some messages for initialization */
  bool consume = true;
  while (consume) {
    rd_kafka_message_t *rkmessage;
    rkmessage = kafkaConsumer->consumer(5000 /* timeout ms */);
    if (rkmessage == nullptr || rkmessage->err) {
      break;
    }
    string topic = rd_kafka_topic_name(rkmessage->rkt);
    string msg((const char *)rkmessage->payload, rkmessage->len);
    if (topic == def()->rawGbtTopic_) {
      processRawGbtMsg(msg);
      consume = false;
    } else if (topic == def()->auxPowGwTopic_) {
      processAuxPowMsg(msg);
    } else if (topic == def()->rskRawGwTopic_) {
      processRskGwMsg(msg);
    } else if (topic == def()->vcashRawGwTopic_) {
      processVcashGwMsg(msg);
    }

    rd_kafka_message_destroy(rkmessage);
  }
  LOG(INFO) << "consume latest rawgbt messages done";

  return true;
}

bool JobMakerHandlerBitcoin::addRawGbt(const string &msg) {
  JsonNode r;
  if (!JsonNode::parse(msg.c_str(), msg.c_str() + msg.size(), r)) {
    LOG(ERROR) << "parse rawgbt message to json fail";
    return false;
  }

  if (r["created_at_ts"].type() != Utilities::JS::type::Int ||
      r["block_template_base64"].type() != Utilities::JS::type::Str ||
      r["gbthash"].type() != Utilities::JS::type::Str) {
    LOG(ERROR) << "invalid rawgbt: missing fields";
    return false;
  }

  const uint256 gbtHash = uint256S(r["gbthash"].str());
  for (const auto &itr : lastestGbtHash_) {
    if (gbtHash == itr) {
      LOG(ERROR) << "duplicate gbt hash: " << gbtHash.ToString();
      return false;
    }
  }

  const uint32_t gbtTime = r["created_at_ts"].uint32();
  const int64_t timeDiff = (int64_t)time(nullptr) - (int64_t)gbtTime;
  if (labs(timeDiff) >= 60) {
    LOG(WARNING) << "rawgbt diff time is more than 60, ignore it";
    return false; // time diff too large, there must be some problems, so ignore
                  // it
  }
  if (labs(timeDiff) >= 3) {
    LOG(WARNING) << "rawgbt diff time is too large: " << timeDiff << " seconds";
  }

  const string gbt = DecodeBase64(r["block_template_base64"].str());
  assert(gbt.length() > 64); // valid gbt string's len at least 64 bytes

  JsonNode nodeGbt;
  if (!JsonNode::parse(gbt.c_str(), gbt.c_str() + gbt.length(), nodeGbt)) {
    LOG(ERROR) << "parse gbt message to json fail";
    return false;
  }
  assert(nodeGbt["result"]["height"].type() == Utilities::JS::type::Int);
  const uint32_t height = nodeGbt["result"]["height"].uint32();

#if defined(CHAIN_TYPE_BCH) || defined(CHAIN_TYPE_BSV)
  bool isLightVersion =
      nodeGbt["result"][LIGHTGBT_JOB_ID].type() == Utilities::JS::type::Str;
  bool isEmptyBlock = false;
  if (isLightVersion) {
    assert(
        nodeGbt["result"][LIGHTGBT_MERKLE].type() ==
        Utilities::JS::type::Array);
    isEmptyBlock = nodeGbt["result"][LIGHTGBT_MERKLE].array().size() == 0;
  } else {
    assert(
        nodeGbt["result"]["transactions"].type() == Utilities::JS::type::Array);
    isEmptyBlock = nodeGbt["result"]["transactions"].array().size() == 0;
  }
#else
  assert(
      nodeGbt["result"]["transactions"].type() == Utilities::JS::type::Array);
  const bool isEmptyBlock =
      nodeGbt["result"]["transactions"].array().size() == 0;
#endif

  if (rawgbtMap_.size() > 0) {
    const uint64_t bestKey = rawgbtMap_.rbegin()->first;
    const uint32_t bestTime = gbtKeyGetTime(bestKey);
    const uint32_t bestHeight = gbtKeyGetHeight(bestKey);

    // To prevent the job's block height ups and downs
    // when the block height of two bitcoind is not synchronized.
    // The block height downs must past twice the time of stratumJobInterval_
    // without the higher height GBT received.
    if (height < bestHeight && gbtTime - bestTime < 2 * def()->jobInterval_) {
      LOG(WARNING) << "skip low height GBT. height: " << height
                   << ", best height: " << bestHeight
                   << ", elapsed time after best GBT: " << (gbtTime - bestTime)
                   << "s";
      return false;
    }
  }

  const uint64_t key = makeGbtKey(gbtTime, isEmptyBlock, height);
  if (rawgbtMap_.find(key) == rawgbtMap_.end()) {
    rawgbtMap_.insert(std::make_pair(key, gbt));
  } else {
    LOG(ERROR) << "key already exist in rawgbtMap: " << key;
  }

  lastestGbtHash_.push_back(gbtHash);
  while (lastestGbtHash_.size() > 20) {
    lastestGbtHash_.pop_front();
  }

  LOG(INFO) << "add rawgbt, height: " << height
            << ", gbthash: " << r["gbthash"].str().substr(0, 16)
            << "..., gbtTime(UTC): " << date("%F %T", gbtTime)
            << ", isEmpty:" << isEmptyBlock;

  return true;
}

bool JobMakerHandlerBitcoin::findBestRawGbt(string &bestRawGbt) {
  static uint64_t lastSendBestKey = 0;

  // clean expired gbt first
  clearTimeoutGbt();
  clearTimeoutGw();
  clearVcashTimeoutGw();

  if (rawgbtMap_.size() == 0) {
    LOG(WARNING) << "RawGbt Map is empty";
    return false;
  }

  bool isFindNewHeight = false;
  bool needUpdateEmptyBlockJob = false;

  // rawgbtMap_ is sorted gbt by (timestamp + height + emptyFlag),
  // so the last item is the newest/best item.
  // @see makeGbtKey()
  const uint64_t bestKey = rawgbtMap_.rbegin()->first;

  const uint32_t bestHeight = gbtKeyGetHeight(bestKey);
  const bool currentGbtIsEmpty = gbtKeyIsEmptyBlock(bestKey);

  if (bestKey == lastSendBestKey) {
    LOG(WARNING) << "bestKey is the same as last one: " << lastSendBestKey;
  }

  // if last job is an empty block job, we need to
  // send a new non-empty job as quick as possible.
  if (bestHeight == currBestHeight_ && isLastJobEmptyBlock_ &&
      !currentGbtIsEmpty) {
    needUpdateEmptyBlockJob = true;
    LOG(INFO) << "--------update last empty block job--------";
  }

  // The height cannot reduce in normal.
  // However, if there is indeed a height reduce,
  // isReachTimeout() will allow the new job sending.
  if (bestHeight > currBestHeight_) {
    LOG(INFO) << ">>>> found new best height: " << bestHeight
              << ", curr: " << currBestHeight_ << " <<<<";
    isFindNewHeight = true;
  }

  if (isFindNewHeight || needUpdateEmptyBlockJob ||
      isReachTimeout()) {
    lastSendBestKey = bestKey;
    currBestHeight_ = bestHeight;

    bestRawGbt = rawgbtMap_.rbegin()->second.c_str();
    return true;
  }

  return false;
}

bool JobMakerHandlerBitcoin::isReachTimeout() {
  uint32_t intervalSeconds = def()->jobInterval_;

  if (lastJobSendTime_ + intervalSeconds <= time(nullptr)) {
    return true;
  }
  return false;
}

void JobMakerHandlerBitcoin::clearTimeoutGbt() {
  // Maps (and sets) are sorted, so the first element is the smallest,
  // and the last element is the largest.

  const uint32_t ts_now = time(nullptr);

  // Ensure that rawgbtMap_ has at least one element, even if it expires.
  // So jobmaker can always generate jobs even if blockchain node does not
  // update the response of getblocktemplate for a long time when there is no
  // new transaction. This happens on SBTC v0.17.
  for (auto itr = rawgbtMap_.begin();
       rawgbtMap_.size() > 1 && itr != rawgbtMap_.end();) {
    const uint32_t ts = gbtKeyGetTime(itr->first);
    const bool isEmpty = gbtKeyIsEmptyBlock(itr->first);
    const uint32_t height = gbtKeyGetHeight(itr->first);

    // gbt expired time
    const uint32_t expiredTime =
        ts + (isEmpty ? def()->emptyGbtLifeTime_ : def()->gbtLifeTime_);

    if (expiredTime > ts_now) {
      // not expired
      ++itr;
    } else {
      // remove expired gbt
      LOG(INFO) << "remove timeout rawgbt: " << date("%F %T", ts) << "|" << ts
                << ", height:" << height
                << ", isEmptyBlock:" << (isEmpty ? 1 : 0);

      // c++11: returns an iterator to the next element in the map
      itr = rawgbtMap_.erase(itr);
    }
  }
}

void JobMakerHandlerBitcoin::clearTimeoutGw() {

}

void JobMakerHandlerBitcoin::clearVcashTimeoutGw() {
}

bool JobMakerHandlerBitcoin::processRawGbtMsg(const string &msg) {
  DLOG(INFO) << "JobMakerHandlerBitcoin::processRawGbtMsg: " << msg;
  return addRawGbt(msg);
}

bool JobMakerHandlerBitcoin::processAuxPowMsg(const string &msg) {
  uint32_t currentNmcBlockHeight = 0;
  string currentNmcBlockHash;
  // get block height
  {
    JsonNode r;
    if (!JsonNode::parse(msg.data(), msg.data() + msg.size(), r)) {
      LOG(ERROR) << "parse NmcAuxBlock message to json fail";
      return false;
    }

    if (r["height"].type() != Utilities::JS::type::Int ||
        r["hash"].type() != Utilities::JS::type::Str) {
      LOG(ERROR) << "nmc auxblock fields failure";
      return false;
    }

    currentNmcBlockHeight = r["height"].uint32();
    currentNmcBlockHash = r["hash"].str();
  }

  // set json string

  // backup old height / hash
  uint32_t latestNmcAuxBlockHeight = latestNmcAuxBlockHeight_;
  string latestNmcAuxBlockHash = latestNmcAuxBlockHash_;
  // update height / hash
  latestNmcAuxBlockHeight_ = currentNmcBlockHeight;
  latestNmcAuxBlockHash_ = currentNmcBlockHash;
  // update json
  latestNmcAuxBlockJson_ = msg;
  DLOG(INFO) << "latestAuxPowJson: " << latestNmcAuxBlockJson_;

  bool higherHeightUpdate = def()->auxmergedMiningNotifyPolicy_ == 1 &&
      currentNmcBlockHeight > latestNmcAuxBlockHeight;

  bool differentHashUpdate = def()->auxmergedMiningNotifyPolicy_ == 2 &&
      currentNmcBlockHash != latestNmcAuxBlockHash;

  bool isMergedMiningUpdate = higherHeightUpdate || differentHashUpdate;

  DLOG_IF(INFO, isMergedMiningUpdate)
      << "it's time to update MergedMining  because aux : "
      << (higherHeightUpdate ? " higherHeightUpdate " : " ")
      << (differentHashUpdate ? " differentHashUpdate " : " ");

  return isMergedMiningUpdate;
}

bool JobMakerHandlerBitcoin::processRskGwMsg(const string &rawGetWork) {
  return false;
}

bool JobMakerHandlerBitcoin::processVcashGwMsg(const string &rawGetWork) {
  return false;
}

string JobMakerHandlerBitcoin::makeStratumJob(const string &gbt) {
  DLOG(INFO) << "JobMakerHandlerBitcoin::makeStratumJob gbt: " << gbt;
  string latestNmcAuxBlockJson = latestNmcAuxBlockJson_;

  StratumJobBitcoin sjob;
  if (!sjob.initFromGbt(
          gbt.c_str(),
          def()->coinbaseInfo_,
          poolPayoutAddr_,
          def()->blockVersion_,
          latestNmcAuxBlockJson,
          false)) {
    LOG(ERROR) << "init stratum job message from gbt str fail";
    return "";
  }
  sjob.jobId_ = gen_->next();
  const string jobMsg = sjob.serializeToJson();

  // set last send time
  // TODO: fix Y2K38 issue
  lastJobSendTime_ = (uint32_t)time(nullptr);

  // is an empty block job
  isLastJobEmptyBlock_ = sjob.isEmptyBlock();

  LOG(INFO) << "--------producer stratum job, jobId: " << sjob.jobId_
            << ", height: " << sjob.height_ << "--------";

  return jobMsg;
}

string JobMakerHandlerBitcoin::makeStratumJobMsg() {
  string bestRawGbt;
  if (!findBestRawGbt(bestRawGbt)) {
    return "";
  }
  return makeStratumJob(bestRawGbt);
}

uint64_t JobMakerHandlerBitcoin::makeGbtKey(
    uint32_t gbtTime, bool isEmptyBlock, uint32_t height) {
  assert(height < 0x7FFFFFFFU);

  // gbtKey: 31 bit height + 1 bit non-empty flag + 32 bit timestamp
  return (((uint64_t)gbtTime)) | (((uint64_t)height) << 33) |
      (((uint64_t)(!isEmptyBlock)) << 32);
}

uint32_t JobMakerHandlerBitcoin::gbtKeyGetTime(uint64_t gbtKey) {
  return (uint32_t)gbtKey;
}

uint32_t JobMakerHandlerBitcoin::gbtKeyGetHeight(uint64_t gbtKey) {
  return (uint32_t)((gbtKey >> 33) & 0x7FFFFFFFULL);
}

bool JobMakerHandlerBitcoin::gbtKeyIsEmptyBlock(uint64_t gbtKey) {
  return !((bool)((gbtKey >> 32) & 1ULL));
}
