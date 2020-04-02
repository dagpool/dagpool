/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#ifndef SHARELOGPARSER_BITCOIN_H_
#define SHARELOGPARSER_BITCOIN_H_

#include "ShareLogParser.h"

#include "StratumQitmeer.h"

///////////////////////////////  Alias  ///////////////////////////////
using ShareLogDumperBitcoin = ShareLogDumperT<ShareBitcoin>;
using ShareLogParserBitcoin = ShareLogParserT<ShareBitcoin>;
using ShareLogParserServerBitcoin = ShareLogParserServerT<ShareBitcoin>;

#endif // SHARELOGPARSER_H_
