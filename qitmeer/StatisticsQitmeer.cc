/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */

#include "StatisticsQitmeer.h"

#include "StratumQitmeer.h"
#include "QitmeerUtils.h"

template <>
double ShareStatsDay<ShareBitcoin>::getShareReward(const ShareBitcoin &share) {
  return GetBlockReward(share.height(), Params().GetConsensus());
}

///////////////  template instantiation ///////////////
// Without this, some linking errors will issued.
// If you add a new derived class of Share, add it at the following.
template class ShareStatsDay<ShareBitcoin>;
// template class ShareStatsDay<ShareBytom>;
