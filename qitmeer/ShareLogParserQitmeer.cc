/*
 * Copyright (c) 2020.
 * Project:dag pool
 * Date:3/25/20 1:43 PM
 * Author:Jin
 * Email:lochjin@gmail.com
 */
#include "ShareLogParserQitmeer.h"

///////////////  template instantiation ///////////////
// Without this, some linking errors will issued.
// If you add a new derived class of Share, add it at the following.
template class ShareLogDumperT<ShareBitcoin>;
template class ShareLogParserT<ShareBitcoin>;
template class ShareLogParserServerT<ShareBitcoin>;
