
#ifndef SFERA_DB_PAGES_CACHE_H
#define SFERA_DB_PAGES_CACHE_H

//----------------------------------------------------------------------------------------------------------------------

#include "db_page.hpp"
#include <unordered_map>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    class pages_cache
    {
    private:
        struct cached_page_info
        {
            bool pinned = true;
            db_page *page = nullptr;

            cached_page_info() { }
            explicit cached_page_info(db_page *p) : page(p) { }
        };


    private:
        size_t _sizePages;
        std::unordered_map<int, cached_page_info> _cachedPages;


    public:
        pages_cache(size_t sizePages);

        db_page* fetchAndPin(int pageId);
        void cacheAndPin(db_page *page);
        void unpin(db_page *page);
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif    //SFERA_DB_PAGES_CACHE_H
