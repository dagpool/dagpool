/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#ifndef QITMEER_UTILS_H_
#define QITMEER_UTILS_H_

#include <string>

#if defined(CHAIN_TYPE_ZEC) && defined(NDEBUG)
// fix "Zcash cannot be compiled without assertions."
#undef NDEBUG
#include <crypto/common.h>
#define NDEBUG
#endif

#include <core_io.h>
// #include <streams.h>
#include <amount.h>
#include <chainparams.h>
#include <base58.h>

#if defined(CHAIN_TYPE_BCH) || defined(CHAIN_TYPE_BSV)

// header that defined DecodeDestination & IsValidDestinationString
#include <dstencode.h>

#ifdef CHAIN_TYPE_BCH

#define AMOUNT_TYPE(x) Amount(x *SATOSHI)
#define COIN_TO_SATOSHIS (COIN / SATOSHI)
#define AMOUNT_SATOSHIS(amt) (amt / SATOSHI)

#define LIGHTGBT_JOB_ID "job_id"
#define LIGHTGBT_PREV_HASH "previousblockhash"
#define LIGHTGBT_BITS "bits"
#define LIGHTGBT_TIME "curtime"
#define LIGHTGBT_COINBASE_VALUE "coinbasevalue"
#define LIGHTGBT_MERKLE "merkle"
#else

#define AMOUNT_TYPE Amount
#define COIN_TO_SATOSHIS COIN.GetSatoshis()
#define AMOUNT_SATOSHIS(amt) amt.GetSatoshis()

#define LIGHTGBT_JOB_ID "id"
#define LIGHTGBT_PREV_HASH "prevhash"
#define LIGHTGBT_BITS "nBits"
#define LIGHTGBT_TIME "time"
#define LIGHTGBT_COINBASE_VALUE "coinbaseValue"
#define LIGHTGBT_MERKLE "merkleProof"
#endif

namespace BitcoinUtils {
inline bool IsValidDestinationString(const std::string &addr) {
  return ::IsValidDestinationString(addr, Params());
}
inline CTxDestination DecodeDestination(const std::string &str) {
  return ::DecodeDestination(str, Params());
}
} // namespace BitcoinUtils

#elif defined(CHAIN_TYPE_SBTC)

#define AMOUNT_TYPE CAmount
#define COIN_TO_SATOSHIS COIN
#define AMOUNT_SATOSHIS(amt) amt

namespace BitcoinUtils {
CTxDestination DecodeDestination(const std::string &str);
bool IsValidDestinationString(const std::string &str);
} // namespace BitcoinUtils

#else

#define AMOUNT_TYPE CAmount
#define COIN_TO_SATOSHIS COIN
#define AMOUNT_SATOSHIS(amt) amt

namespace BitcoinUtils {
inline bool IsValidDestinationString(const std::string &addr) {
  return ::IsValidDestinationString(addr);
}
inline CTxDestination DecodeDestination(const std::string &str) {
  return ::DecodeDestination(str);
}
} // namespace BitcoinUtils

#endif

#ifdef CHAIN_TYPE_ZEC
int32_t getSolutionVintSize();
bool CheckEquihashSolution(
    const CBlockHeader *pblock, const CChainParams &params);
#endif

#if defined(CHAIN_TYPE_BCH) || defined(CHAIN_TYPE_ZEC)
std::vector<uint256>
ComputeMerkleBranch(const std::vector<uint256> &leaves, uint32_t position);
std::vector<uint256> BlockMerkleBranch(const CBlock &block, uint32_t position);
#endif

uint256 ComputeCoinbaseMerkleRoot(
    const std::vector<char> &coinbaseBin,
    const std::vector<uint256> &merkleBranch);

std::string EncodeHexBlock(const CBlock &block);
std::string EncodeHexBlockHeader(const CBlockHeader &blkHeader);

int64_t GetBlockReward(int nHeight, const Consensus::Params &consensusParams);

bool checkRPC(const std::string &rpcAddr, const std::string &rpcUserpass);

int32_t getBlockHeightFromCoinbase(const std::string &coinbase1);

std::string getNotifyHashStr(const uint256 &hash);
std::string getNotifyUint32Str(const uint32_t var);

inline uint16_t SwapUint(uint16_t v) {
  return (v >> 8) | (v << 8);
}
inline uint32_t SwapUint(uint32_t v) {
  return ((v & 0xff000000) >> 24) | ((v & 0x00ff0000) >> 8) |
      ((v & 0x0000ff00) << 8) | ((v & 0x000000ff) << 24);
}
inline uint64_t SwapUint(uint64_t v) {
  return ((v & 0xff00000000000000ULL) >> 56) |
      ((v & 0x00ff000000000000ULL) >> 40) |
      ((v & 0x0000ff0000000000ULL) >> 24) | ((v & 0x000000ff00000000ULL) >> 8) |
      ((v & 0x00000000ff000000ULL) << 8) | ((v & 0x0000000000ff0000ULL) << 24) |
      ((v & 0x000000000000ff00ULL) << 40) | ((v & 0x00000000000000ffULL) << 56);
}
uint256 SwapUint(const uint256 &hash);

uint256 reverse8bit(uint256 &&hash);
uint256 reverse8bit(uint256 &&hash);
uint256 reverse32bit(uint256 &&hash);

std::string reverse16bit(std::string &&hash);
std::string reverse16bit(const std::string &hash);

#endif // QITMEER_UTILS_H_
