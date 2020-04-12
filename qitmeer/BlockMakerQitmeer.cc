/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#include "BlockMakerQitmeer.h"

#include "StratumQitmeer.h"

#include "QitmeerUtils.h"


#include <consensus/merkle.h>

#include <boost/thread.hpp>

#include <streams.h>

////////////////////////////////// BlockMaker //////////////////////////////////
BlockMakerQitmeer::BlockMakerQitmeer(
    shared_ptr<BlockMakerDefinition> blkMakerDef,
    const char *kafkaBrokers,
    const MysqlConnectInfo &poolDB)
  : BlockMaker(blkMakerDef, kafkaBrokers, poolDB)
  , kMaxRawGbtNum_(100) /* if 5 seconds a rawgbt, will hold 100*5/60 = 8 mins rawgbt */
  , kMaxStratumJobNum_(120) /* if 30 seconds a stratum job, will hold 60 mins stratum job */
  , lastSubmittedBlockTime()
  , kafkaConsumerRawGbt_(kafkaBrokers, def()->rawGbtTopic_.c_str(), 0 /* patition */)
  , kafkaConsumerStratumJob_(kafkaBrokers, def()->stratumJobTopic_.c_str(), 0 /* patition */)
{
}

BlockMakerQitmeer::~BlockMakerQitmeer() {
  if (threadConsumeRawGbt_.joinable())
    threadConsumeRawGbt_.join();

  if (threadConsumeStratumJob_.joinable())
    threadConsumeStratumJob_.join();


}

bool BlockMakerQitmeer::init() {
  if (!check())
    return false;

  if (!BlockMaker::init()) {
    return false;
  }
  //
  // Raw Gbt
  //
  // we need to consume the latest N messages
  if (kafkaConsumerRawGbt_.setup(RD_KAFKA_OFFSET_TAIL(kMaxRawGbtNum_)) ==
      false) {
    LOG(INFO) << "setup kafkaConsumerRawGbt_ fail";
    return false;
  }
  if (!kafkaConsumerRawGbt_.checkAlive()) {
    LOG(ERROR) << "kafka brokers is not alive: kafkaConsumerRawGbt_";
    return false;
  }

  //
  // Stratum Job
  //
  // we need to consume the latest 2 messages, just in case
  if (kafkaConsumerStratumJob_.setup(
          RD_KAFKA_OFFSET_TAIL(kMaxStratumJobNum_)) == false) {
    LOG(INFO) << "setup kafkaConsumerStratumJob_ fail";
    return false;
  }
  if (!kafkaConsumerStratumJob_.checkAlive()) {
    LOG(ERROR) << "kafka brokers is not alive: kafkaConsumerStratumJob_";
    return false;
  }
  return true;
}

void BlockMakerQitmeer::run() {
  // setup threads
  threadConsumeRawGbt_ =
          std::thread(&BlockMakerQitmeer::runThreadConsumeRawGbt, this);
  threadConsumeStratumJob_ =
          std::thread(&BlockMakerQitmeer::runThreadConsumeStratumJob, this);

  BlockMaker::run();
}

void BlockMakerQitmeer::runThreadConsumeRawGbt() {
  const int32_t timeoutMs = 1000;

  while (running_) {
    rd_kafka_message_t *rkmessage;
    rkmessage = kafkaConsumerRawGbt_.consumer(timeoutMs);
    if (rkmessage == nullptr) /* timeout */
      continue;

    consumeRawGbt(rkmessage);

    /* Return message to rdkafka */
    rd_kafka_message_destroy(rkmessage);
  }
}

void BlockMakerQitmeer::runThreadConsumeStratumJob() {
  const int32_t timeoutMs = 1000;

  while (running_) {
    rd_kafka_message_t *rkmessage;
    rkmessage = kafkaConsumerStratumJob_.consumer(timeoutMs);
    if (rkmessage == nullptr) /* timeout */
      continue;

    consumeStratumJob(rkmessage);

    /* Return message to rdkafka */
    rd_kafka_message_destroy(rkmessage);
  }
}


void BlockMakerQitmeer::consumeRawGbt(rd_kafka_message_t *rkmessage) {
  // check error
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      // Reached the end of the topic+partition queue on the broker.
      // Not really an error.
      //      LOG(INFO) << "consumer reached end of " <<
      //      rd_kafka_topic_name(rkmessage->rkt)
      //      << "[" << rkmessage->partition << "] "
      //      << " message queue at offset " << rkmessage->offset;
      // acturlly
      return;
    }

    LOG(ERROR) << "consume error for topic "
               << rd_kafka_topic_name(rkmessage->rkt) << "["
               << rkmessage->partition << "] offset " << rkmessage->offset
               << ": " << rd_kafka_message_errstr(rkmessage);

    if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
        rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
      LOG(FATAL) << "consume fatal";
      stop();
    }
    return;
  }

  LOG(INFO) << "received rawgbt message, len: " << rkmessage->len;
  addRawgbt((const char *)rkmessage->payload, rkmessage->len);
}

void BlockMakerQitmeer::consumeStratumJob(rd_kafka_message_t *rkmessage) {
  // check error
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      // Reached the end of the topic+partition queue on the broker.
      // Not really an error.
      //      LOG(INFO) << "consumer reached end of " <<
      //      rd_kafka_topic_name(rkmessage->rkt)
      //      << "[" << rkmessage->partition << "] "
      //      << " message queue at offset " << rkmessage->offset;
      // acturlly
      return;
    }

    LOG(ERROR) << "consume error for topic "
               << rd_kafka_topic_name(rkmessage->rkt) << "["
               << rkmessage->partition << "] offset " << rkmessage->offset
               << ": " << rd_kafka_message_errstr(rkmessage);

    if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
        rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
      LOG(FATAL) << "consume fatal";
      stop();
    }
    return;
  }

  LOG(INFO) << "received StratumJob message, len: " << rkmessage->len;

  shared_ptr<StratumJobBitcoin> sjob = std::make_shared<StratumJobBitcoin>();
  bool res = sjob->unserializeFromJson(
          (const char *)rkmessage->payload, rkmessage->len);
  if (res == false) {
    LOG(ERROR) << "unserialize stratum job fail";
    return;
  }

  const uint256 gbtHash = uint256S(sjob->gbtHash_);
  {
    ScopeLock sl(jobIdMapLock_);
    jobId2GbtHash_[sjob->jobId_] = gbtHash;

    // Maps (and sets) are sorted, so the first element is the smallest,
    // and the last element is the largest.
    while (jobId2GbtHash_.size() > kMaxStratumJobNum_) {
      jobId2GbtHash_.erase(jobId2GbtHash_.begin());
    }
  }


  LOG(INFO) << "StratumJob, jobId: " << sjob->jobId_
            << ", gbtHash: " << gbtHash.ToString();

}

void BlockMakerQitmeer::processSolvedShare(rd_kafka_message_t *rkmessage) {
  //
  // solved share message:  FoundBlock + coinbase_Tx
  //
  FoundBlock foundBlock;
  CBlockHeader blkHeader;
  vector<char> coinbaseTxBin;

  {
    if (rkmessage->len <= sizeof(FoundBlock)) {
      LOG(ERROR) << "invalid SolvedShare length: " << rkmessage->len;
      return;
    }
    coinbaseTxBin.resize(rkmessage->len - sizeof(FoundBlock));

    // foundBlock
    memcpy(
            (uint8_t *)&foundBlock,
            (const uint8_t *)rkmessage->payload,
            sizeof(FoundBlock));

    // coinbase tx
    memcpy(
            (uint8_t *)coinbaseTxBin.data(),
            (const uint8_t *)rkmessage->payload + sizeof(FoundBlock),
            coinbaseTxBin.size());
    // copy header
    foundBlock.headerData_.get(blkHeader);
  }

  // get gbtHash and rawgbt (vtxs)
  uint256 gbtHash;
  shared_ptr<vector<CTransactionRef>> vtxs;
  {
    ScopeLock sl(jobIdMapLock_);
    if (jobId2GbtHash_.find(foundBlock.jobId_) != jobId2GbtHash_.end()) {
      gbtHash = jobId2GbtHash_[foundBlock.jobId_];
    }
  }


  {
    ScopeLock ls(rawGbtLock_);
    if (rawGbtMap_.find(gbtHash) == rawGbtMap_.end()) {
      LOG(ERROR) << "can't find this gbthash in rawGbtMap_: "
                 << gbtHash.ToString();
      return;
    }
    vtxs = rawGbtMap_[gbtHash];
    assert(vtxs.get() != nullptr);
  }

  //
  // build new block
  //
  CBlock newblk(blkHeader);

  // put coinbase tx
  {
    CSerializeData sdata;
    sdata.insert(sdata.end(), coinbaseTxBin.begin(), coinbaseTxBin.end());

    newblk.vtx.push_back(MakeTransactionRef());

    CDataStream c(sdata, SER_NETWORK, PROTOCOL_VERSION);
    c >> newblk.vtx[newblk.vtx.size() - 1];
  }

  // put other txs
  if (vtxs && vtxs->size()) {

    newblk.vtx.insert(newblk.vtx.end(), vtxs->begin(), vtxs->end());

  }

  // submit to qitmeer
  const string blockHex = EncodeHexBlock(newblk);
  LOG(INFO) << "submit block: " << newblk.GetHash().ToString();
  submitBlockNonBlocking(blockHex); // using thread

  uint64_t coinbaseValue = AMOUNT_SATOSHIS(newblk.vtx[0]->GetValueOut());

  // save to DB, using thread
  saveBlockToDBNonBlocking(
          foundBlock,
          blkHeader,
          coinbaseValue, // coinbase value
          blockHex.length() / 2);
}

void BlockMakerQitmeer::addRawgbt(const char *str, size_t len) {
  JsonNode r;
  if (!JsonNode::parse(str, str + len, r)) {
    LOG(ERROR) << "parse rawgbt message to json fail";
    return;
  }
  if (r["created_at_ts"].type() != Utilities::JS::type::Int ||
      r["block_template_base64"].type() != Utilities::JS::type::Str ||
      r["gbthash"].type() != Utilities::JS::type::Str) {
    LOG(ERROR) << "invalid rawgbt: missing fields";
    return;
  }

  const uint256 gbtHash = uint256S(r["gbthash"].str());
  if (rawGbtMap_.find(gbtHash) != rawGbtMap_.end()) {
    LOG(ERROR) << "already exist raw gbt, ignore: " << gbtHash.ToString();
    return;
  }

  const string gbt = DecodeBase64(r["block_template_base64"].str());
  assert(gbt.length() > 64); // valid gbt string's len at least 64 bytes

  JsonNode nodeGbt;
  if (!JsonNode::parse(gbt.c_str(), gbt.c_str() + gbt.length(), nodeGbt)) {
    LOG(ERROR) << "parse gbt message to json fail";
    return;
  }
  JsonNode jgbt = nodeGbt["result"];


  // transaction without coinbase_tx
  shared_ptr<vector<CTransactionRef>> vtxs =
      std::make_shared<vector<CTransactionRef>>();
  for (JsonNode &node : jgbt["transactions"].array()) {

    CMutableTransaction tx;
    DecodeHexTx(tx, node["data"].str());
    vtxs->push_back(MakeTransactionRef(std::move(tx)));

  }

  LOG(INFO) << "insert rawgbt: " << gbtHash.ToString()
            << ", txs: " << vtxs->size();
  insertRawGbt(gbtHash, vtxs);
}

void BlockMakerQitmeer::insertRawGbt(
    const uint256 &gbtHash, shared_ptr<vector<CTransactionRef>> vtxs) {
  ScopeLock ls(rawGbtLock_);

  // insert rawgbt
  rawGbtMap_[gbtHash] = vtxs;
  rawGbtQ_.push_back(gbtHash);

  // remove rawgbt if need
  while (rawGbtQ_.size() > kMaxRawGbtNum_) {
    const uint256 h = *rawGbtQ_.begin();

    rawGbtMap_.erase(h); // delete from map
    rawGbtQ_.pop_front(); // delete from Q
  }
}

void BlockMakerQitmeer::saveBlockToDBNonBlocking(
    const FoundBlock &foundBlock,
    const CBlockHeader &header,
    const uint64_t coinbaseValue,
    const int32_t blksize) {
  std::thread t(std::bind(
      &BlockMakerQitmeer::_saveBlockToDBThread,
      this,
      foundBlock,
      header,
      coinbaseValue,
      blksize));
  t.detach();
}

void BlockMakerQitmeer::_saveBlockToDBThread(
    const FoundBlock &foundBlock,
    const CBlockHeader &header,
    const uint64_t coinbaseValue,
    const int32_t blksize) {
  const string nowStr = date("%F %T");
  string sql;
  sql = Strings::Format(
      "INSERT INTO `found_blocks` "
      " (`puid`, `worker_id`, `worker_full_name`, `job_id`"
      "  ,`height`, `hash`, `rewards`, `size`, `prev_hash`"
      "  ,`bits`, `version`, `created_at`)"
      " VALUES (%d,%d,\"%s\",%u,%d,\"%s\",%d,%d,\"%s\",%u,%d,\"%s\"); ",
      foundBlock.userId_,
      foundBlock.workerId_,
      // filter again, just in case
      filterWorkerName(foundBlock.workerFullName_),
      foundBlock.jobId_,
      foundBlock.height_,
      header.GetHash().ToString(),
      coinbaseValue,
      blksize,
      header.hashPrevBlock.ToString(),
      header.nBits,
      header.nVersion,
      nowStr);

  LOG(INFO) << "BlockMakerQitmeer::_saveBlockToDBThread: " << sql;

  // try connect to DB
  MySQLConnection db(poolDB_);
  for (size_t i = 0; i < 3; i++) {
    if (db.ping())
      break;
    else
      std::this_thread::sleep_for(3s);
  }

  if (db.execute(sql) == false) {
    LOG(ERROR) << "insert found block failure: " << sql;
  }
}

bool BlockMakerQitmeer::check() {
  if (def()->nodes.size() == 0) {
    return false;
  }

  for (const auto &itr : def()->nodes) {
    if (!checkRPC(itr.rpcAddr_.c_str(), itr.rpcUserPwd_.c_str())) {
      return false;
    }
  }

  return true;
}

void BlockMakerQitmeer::submitBlockNonBlocking(const string &blockHex) {
  for (const auto &itr : def()->nodes) {
    // use thread to submit
    std::thread t(std::bind(
        &BlockMakerQitmeer::_submitBlockThread,
        this,
        itr.rpcAddr_,
        itr.rpcUserPwd_,
        blockHex));
    t.detach();
  }
}

void BlockMakerQitmeer::_submitBlockThread(
    const string &rpcAddress,
    const string &rpcUserpass,
    const string &blockHex) {
  string request =
      "{\"jsonrpc\":\"1.0\",\"id\":\"1\",\"method\":\"submitblock\",\"params\":"
      "[\"";
  request += blockHex + "\"]}";

  LOG(INFO) << "submit block to: " << rpcAddress;
  DLOG(INFO) << "submitblock request: " << request;
  // try N times
  for (size_t i = 0; i < 3; i++) {
    string response;
    bool res = blockchainNodeRpcCall(
        rpcAddress.c_str(), rpcUserpass.c_str(), request.c_str(), response);

    // success
    if (res == true) {
      LOG(INFO) << "rpc call success, submit block response: " << response;
      break;
    }

    // failure
    LOG(ERROR) << "rpc call fail: " << response
               << "\nrpc request : " << request;
  }
}