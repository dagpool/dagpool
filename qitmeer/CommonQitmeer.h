/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#ifndef POOL_COMMON_BITCOIN_H_
#define POOL_COMMON_BITCOIN_H_

#include "Difficulty.h"

#ifdef CHAIN_TYPE_LTC
using BitcoinDifficulty = Difficulty<0x1f00ffff>;
#elif defined(CHAIN_TYPE_ZEC)
using BitcoinDifficulty = Difficulty<0x1f07ffff>;
#else
using BitcoinDifficulty = Difficulty<0x1d00ffff>;
#endif

#endif
