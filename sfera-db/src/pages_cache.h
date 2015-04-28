
#ifndef SFERA_DB_PAGES_CACHE_H
#define SFERA_DB_PAGES_CACHE_H

//----------------------------------------------------------------------------------------------------------------------

#include "db_page.hpp"

#include <unordered_map>
#include <functional>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    class pages_cache
    {
    private:
        struct cached_page_info
        {
            bool pinned = true;
            bool permanent = false;
            bool dirty = false;
            db_page *page = nullptr;

            cached_page_info() { }
            explicit cached_page_info(db_page *p) : page(p) { }
        };


    private:
        std::function<void(db_page*)> _pageWriter;
        size_t _sizePages;
        std::unordered_map<int, cached_page_info> _cachedPages;


    private:
        void _writeAndDestroyPage(const cached_page_info &pageInfo);

    public:
        pages_cache(size_t sizePages, std::function<void(db_page*)> pageWriter);
        ~pages_cache();

        void flush();
        void clearCache();

        db_page* fetchAndPin(int pageId);
        void cacheAndPin(db_page *page);
        void makeDirty(db_page *page);
        void makePermanent(db_page *page);
        void unpin(db_page *page);
        void invalidateCachedPage(int pageId);
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif    //SFERA_DB_PAGES_CACHE_H
