
#ifndef SFERA_DB_CACHED_PAGE_INFO_HPP
#define SFERA_DB_CACHED_PAGE_INFO_HPP

//----------------------------------------------------------------------------------------------------------------------

#include <unordered_map>
#include <functional>
#include <list>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
    namespace pages_cache_internals
    {

        struct cached_page_info
        {
            int pinned = 0;
            bool dirty = false;
            std::list<int>::const_iterator lruQueueIterator;


            cached_page_info() { }
            inline bool isUsed() const  { return pinned != 0; }
        };

    }
}

//----------------------------------------------------------------------------------------------------------------------

#endif //SFERA_DB_CACHED_PAGE_INFO_HPP
