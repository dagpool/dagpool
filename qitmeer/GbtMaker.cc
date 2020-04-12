/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#include "GbtMaker.h"

#include "QitmeerUtils.h"

#include <glog/logging.h>

#include <util.h>

#include "Utils.h"
#include "utilities_js.hpp"
#include "hash.h"

//
// bitcoind zmq pub msg type: "hashblock", "hashtx", "rawblock", "rawtx"
//
static const std::string BITCOIND_ZMQ_HASHBLOCK = "hashblock";
static const std::string BITCOIND_ZMQ_HASHTX = "hashtx";

//
// namecoind zmq pub msg type: "hashblock", "hashtx", "rawblock", "rawtx"
//
static const std::string NAMECOIND_ZMQ_HASHBLOCK = "hashblock";
static const std::string NAMECOIND_ZMQ_HASHTX = "hashtx";

static bool CheckZmqPublisher(
    zmq::context_t &context,
    const std::string &address,
    const std::string &msgType) {
  zmq::socket_t subscriber(context, ZMQ_SUB);
  subscriber.connect(address);
  subscriber.setsockopt(ZMQ_SUBSCRIBE, msgType.c_str(), msgType.size());
  zmq::message_t ztype, zcontent;

  LOG(INFO) << "check " << address << " zmq, waiting for zmq message '"
            << msgType << "'...";
  try {
    subscriber.recv(&ztype);
  } catch (zmq::error_t &e) {
    LOG(ERROR) << address << " zmq recv exception: " << e.what();
    return false;
  }
  const string type =
      std::string(static_cast<char *>(ztype.data()), ztype.size());

  if (type == msgType) {
    subscriber.recv(&zcontent);
    string content;
    Bin2Hex(static_cast<uint8_t *>(zcontent.data()), zcontent.size(), content);
    LOG(INFO) << address << " zmq recv " << type << ": " << content;
    return true;
  }

  LOG(ERROR) << "unknown zmq message type from " << address << ": " << type;
  return false;
}

static void ListenToZmqPublisher(
    zmq::context_t &context,
    const std::string &address,
    const std::string &msgType,
    const std::atomic<bool> &running,
    uint32_t timeout,
    std::function<void()> callback) {
  int timeoutMs = timeout * 1000;
  LOG_IF(FATAL, timeoutMs <= 0) << "zmq timeout has to be positive!";

  while (running) {
    zmq::socket_t subscriber(context, ZMQ_SUB);
    subscriber.connect(address);
    subscriber.setsockopt(ZMQ_SUBSCRIBE, msgType.c_str(), msgType.size());
    subscriber.setsockopt(ZMQ_RCVTIMEO, &timeoutMs, sizeof(timeout));

    while (running) {
      zmq::message_t zType, zContent;
      try {
        // use block mode with receive timeout
        if (subscriber.recv(&zType) == false) {
          LOG(WARNING) << "zmq recv timeout, reconnecting to " << address;
          break;
        }
      } catch (zmq::error_t &e) {
        LOG(ERROR) << address << " zmq recv exception: " << e.what();
        break; // break big while
      }

      if (0 ==
          msgType.compare(
              0,
              msgType.size(),
              static_cast<char *>(zType.data()),
              zType.size())) {
        subscriber.recv(&zContent);
        string content;
        Bin2Hex(
            static_cast<uint8_t *>(zContent.data()), zContent.size(), content);
        LOG(INFO) << ">>>> " << address << " zmq recv " << msgType << ": "
                  << content << " <<<<";
        LOG(INFO) << "get zmq message, call rpc getblocktemplate";
        callback();
      }
      // Ignore any unknown fields to keep forward compatible.
      // Message sender may add new fields in the future.

    } /* /while */

    subscriber.close();
  }
  LOG(INFO) << "stop thread listen to bitcoind";
}

///////////////////////////////////  GbtMaker  /////////////////////////////////
GbtMaker::GbtMaker(
    const string &zmqBitcoindAddr,
    uint32_t zmqTimeout,
    const string &bitcoindRpcAddr,
    const string &bitcoindRpcUserpass,
    const string &kafkaBrokers,
    const string &kafkaRawGbtTopic,
    uint32_t kRpcCallInterval,
    bool isCheckZmq)
  : running_(true)
  , zmqContext_(std::make_unique<zmq::context_t>(1 /*i/o threads*/))
  , zmqBitcoindAddr_(zmqBitcoindAddr)
  , zmqTimeout_(zmqTimeout)
  , bitcoindRpcAddr_(bitcoindRpcAddr)
  , bitcoindRpcUserpass_(bitcoindRpcUserpass)
  , lastGbtMakeTime_(0)
  , kRpcCallInterval_(kRpcCallInterval)
  , kafkaBrokers_(kafkaBrokers)
  , kafkaRawGbtTopic_(kafkaRawGbtTopic)
  , kafkaProducer_(
        kafkaBrokers_.c_str(), kafkaRawGbtTopic_.c_str(), 0 /* partition */)
  , isCheckZmq_(isCheckZmq) {

}

GbtMaker::~GbtMaker() {
}

bool GbtMaker::init() {
  map<string, string> options;
  // set to 1 (0 is an illegal value here), deliver msg as soon as possible.
  options["queue.buffering.max.ms"] = "1";
  if (!kafkaProducer_.setup(&options)) {
    LOG(ERROR) << "kafka producer setup failure";
    return false;
  }

  // setup kafka and check if it's alive
  if (!kafkaProducer_.checkAlive()) {
    LOG(ERROR) << "kafka is NOT alive";
    return false;
  }

  // check bitcoind network
  if (!checkRPC(
          bitcoindRpcAddr_.c_str(), bitcoindRpcUserpass_.c_str())) {
    return false;
  }

  if (isCheckZmq_ &&
      !CheckZmqPublisher(*zmqContext_, zmqBitcoindAddr_, BITCOIND_ZMQ_HASHTX))
    return false;

  return true;
}

void GbtMaker::stop() {
  if (!running_) {
    return;
  }
  running_ = false;
  zmqContext_.reset();
  LOG(INFO) << "stop gbtmaker";
}

void GbtMaker::kafkaProduceMsg(const void *payload, size_t len) {
  kafkaProducer_.produce(payload, len);
}

bool GbtMaker::bitcoindRpcGBT(string &response) {
  string request =
      "{\"jsonrpc\":\"1.0\",\"id\":\"1\",\"method\":\"getblocktemplate\","
      "\"params\":[{\"rules\" : [\"segwit\"]}]}";
  bool res = blockchainNodeRpcCall(
      bitcoindRpcAddr_.c_str(),
      bitcoindRpcUserpass_.c_str(),
      request.c_str(),
      response);
  if (!res) {
    LOG(ERROR) << "bitcoind rpc failure";
    return false;
  }
  return true;
}

string GbtMaker::makeRawGbtMsg() {
  string gbt;
  if (!bitcoindRpcGBT(gbt)) {
    return "";
  }

  JsonNode r;
  if (!JsonNode::parse(gbt.c_str(), gbt.c_str() + gbt.length(), r)) {
    LOG(ERROR) << "decode gbt failure: " << gbt;
    return "";
  }

  // check fields
  if (r["result"].type() != Utilities::JS::type::Obj ||
      r["result"]["previousblockhash"].type() != Utilities::JS::type::Str ||
      r["result"]["height"].type() != Utilities::JS::type::Int ||

      r["result"]["coinbasevalue"].type() != Utilities::JS::type::Int ||
      r["result"]["bits"].type() != Utilities::JS::type::Str ||
      r["result"]["mintime"].type() != Utilities::JS::type::Int ||
      r["result"]["curtime"].type() != Utilities::JS::type::Int ||
      r["result"]["version"].type() != Utilities::JS::type::Int) {
    LOG(ERROR) << "gbt check fields failure";
    return "";
  }
  const uint256 gbtHash = Hash(gbt.begin(), gbt.end());

  LOG(INFO) << "gbt height: " << r["result"]["height"].uint32()
            << ", prev_hash: " << r["result"]["previousblockhash"].str()
            << ", coinbase_value: " << r["result"]["coinbasevalue"].uint64()
            << ", bits: " << r["result"]["bits"].str()
            << ", mintime: " << r["result"]["mintime"].uint32()
            << ", version: " << r["result"]["version"].uint32() << "|0x"
            << Strings::Format("%08x", r["result"]["version"].uint32())
            << ", gbthash: " << gbtHash.ToString();

  return Strings::Format(
      "{\"created_at_ts\":%u,"
      "\"block_template_base64\":\"%s\","
      "\"gbthash\":\"%s\"}",
      (uint32_t)time(nullptr),
      EncodeBase64(gbt),
      gbtHash.ToString());
  //  return Strings::Format("{\"created_at_ts\":%u,"
  //                         "\"gbthash\":\"%s\"}",
  //                         (uint32_t)time(nullptr),
  //                         gbtHash.ToString());
}

void GbtMaker::submitRawGbtMsg(bool checkTime) {
  ScopeLock sl(lock_);

  if (checkTime && lastGbtMakeTime_ + kRpcCallInterval_ > time(nullptr)) {
    return;
  }

  const string rawGbtMsg = makeRawGbtMsg();
  if (rawGbtMsg.length() == 0) {
    LOG(ERROR) << "get rawgbt failure";
    return;
  }
  lastGbtMakeTime_ = (uint32_t)time(nullptr);

  // submit to Kafka
  LOG(INFO) << "sumbit to Kafka, msg len: " << rawGbtMsg.size();
  kafkaProduceMsg(rawGbtMsg.data(), rawGbtMsg.size());
}

void GbtMaker::threadListenBitcoind() {
  ListenToZmqPublisher(
      *zmqContext_,
      zmqBitcoindAddr_,
      BITCOIND_ZMQ_HASHBLOCK,
      running_,
      zmqTimeout_,
      [this]() { submitRawGbtMsg(false); });
}

void GbtMaker::run() {
  auto threadListenBitcoind =
      std::thread(&GbtMaker::threadListenBitcoind, this);

  while (running_) {
    std::this_thread::sleep_for(1s);
    submitRawGbtMsg(true);
  }

  if (threadListenBitcoind.joinable())
    threadListenBitcoind.join();
}
