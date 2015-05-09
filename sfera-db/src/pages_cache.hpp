
#ifndef SFERA_DB_PAGES_CACHE_H
#define SFERA_DB_PAGES_CACHE_H

//----------------------------------------------------------------------------------------------------------------------

#include "db_page.hpp"

#include <unordered_map>
#include <functional>
#include <list>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    class pages_cache
    {
    public:
        struct statistics_t
        {
            size_t ecivtionsCount  = 0;
            size_t missesCount     = 0;
            size_t fetchesCount    = 0;
            size_t failedEvictions = 0;
        };

    private:
        struct cached_page_info
        {
            int pinned = 0;
            bool permanent = false;
            bool dirty = false;
            std::list<int>::const_iterator lruQueueIterator;

            db_page *page = nullptr;

            cached_page_info() { }
            explicit cached_page_info(db_page *p) : page(p) { }
        };


    private:
        statistics_t _statistics;
        std::function<void(db_page*)> _pageWriter;
        size_t _sizePages;

        std::unordered_map<int, cached_page_info> _cachedPages;
        std::list<int> _lruQueue;


    private:
        void _writeAndDestroyPage(const cached_page_info &pageInfo);
        void _evict();
        void _lruAdd(cached_page_info &pageInfo);
        void _lruAccess(cached_page_info &pageInfo);

    public:
        pages_cache(size_t sizePages, std::function<void(db_page*)> pageWriter);
        ~pages_cache();

        void flush();
        void clearCache();

        db_page* fetchAndPin(int pageId);
        void cacheAndPin(db_page *page);
        void pin(db_page* page);
        void makeDirty(db_page *page);
        void makePermanent(db_page *page);
        void makeEvictable(db_page *page);
        void unpin(db_page *page);
        void unpinIfClean(db_page *page);
        void invalidateCachedPage(int pageId);

        inline const statistics_t& statictics() const  { return _statistics; }
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif    //SFERA_DB_PAGES_CACHE_H
