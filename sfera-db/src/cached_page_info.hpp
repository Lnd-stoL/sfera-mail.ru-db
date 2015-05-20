
#ifndef SFERA_DB_CACHED_PAGE_INFO_HPP
#define SFERA_DB_CACHED_PAGE_INFO_HPP

//----------------------------------------------------------------------------------------------------------------------

#include <unordered_map>
#include <functional>
#include <list>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
    class db_page;

    //----------------------------------------------------------------------------------------------------------------------

    namespace pages_cache_internals
    {

        struct cached_page_info
        {
            int pinned = 0;
            bool dirty = false;
            std::list<int>::const_iterator lruQueueIterator;

            db_page *page = nullptr;

            cached_page_info() { }
            explicit cached_page_info(db_page *p) : page(p) { }
        };

    }
}

//----------------------------------------------------------------------------------------------------------------------

#endif //SFERA_DB_CACHED_PAGE_INFO_HPP
