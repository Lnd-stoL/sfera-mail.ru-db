
#ifndef SFERA_DB_PAGES_CACHE_H
#define SFERA_DB_PAGES_CACHE_H

//----------------------------------------------------------------------------------------------------------------------

#include "db_page.hpp"
#include "cached_page_info.hpp"

#include <unordered_map>
#include <functional>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <queue>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    using pages_cache_internals::cached_page_info;

//----------------------------------------------------------------------------------------------------------------------

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
        statistics_t _statistics;
        std::function<void(db_page*)> _pageWriter;
        size_t _sizePages;

        std::unordered_map<int, db_page*> _cachedPages;
        std::list<int> _lruQueue;

        /*
        bool _writerThreadWorking = true;
        std::thread* _writerThread;
        std::queue<db_page*> _diskWriteQueue;
        std::mutex _writeQueueMutex;
        std::mutex _pagesCacheMutex;
        std::condition_variable _writeQueueCondVar;
        bool _pageToWriteAvaliable = false;
        */

    private:
        void _finalizePage(db_page *page);
        bool _evict();
        void _lruAdd(db_page *page);
        void _lruAccess(db_page *page);

        void _enqueuePageWrite(db_page* page);
        void _removeFromCache(db_page *page);
        void _writerThreadRoutine();

    public:
        pages_cache(size_t sizePages, std::function<void(db_page*)> pageWriter);
        ~pages_cache();

        void flush();
        void clearCache();

        db_page* fetchAndPin(int pageId);
        void cacheAndPin(db_page *page);
        void invalidateCachedPage(int pageId);
        void makeDirty(db_page *page);

        void pin(db_page* page);
        void unpin(db_page *page);

        inline const statistics_t &statistics() const  { return _statistics; }
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif    //SFERA_DB_PAGES_CACHE_H
