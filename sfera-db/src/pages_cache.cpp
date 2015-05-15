
#include "pages_cache.hpp"
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

pages_cache::pages_cache(size_t sizePages, std::function<void(db_page*)> pageWriter) :
    _sizePages(sizePages),
    _pageWriter(pageWriter)
{  }


db_page* pages_cache::fetchAndPin(int pageId)
{
    _statistics.fetchesCount++;

    auto pageIt = _cachedPages.find(pageId);
    if (pageIt == _cachedPages.end()) {
        _statistics.missesCount++;
        return nullptr;
    }

    pageIt->second.pinned = 1;
    _lruAccess(pageIt->second);

    return pageIt->second.page;
}


void pages_cache::cacheAndPin(db_page *page)
{
    assert( page != nullptr );
    auto pageIt = _cachedPages.find(page->id());

    if (pageIt == _cachedPages.end()) {
        cached_page_info newPageInfo(page);
        newPageInfo.pinned = 1;
        _lruAdd(newPageInfo);
        _cachedPages[page->id()] = newPageInfo;
    }
}


void pages_cache::unpin(db_page *page)
{
    assert( page != nullptr );

    auto pageIt = _cachedPages.find(page->id());
    if (pageIt == _cachedPages.end()) return;

    pageIt->second.pinned = 0;
}


void pages_cache::invalidateCachedPage(int pageId)
{
    cached_page_info &pageInfo = _cachedPages[pageId];
    _lruQueue.erase(pageInfo.lruQueueIterator);

    _writeAndDestroyPage(pageInfo);
    _cachedPages.erase(pageId);
}


void pages_cache::makeDirty(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    assert( pageIt != _cachedPages.end() );

    pageIt->second.dirty = true;
}


void pages_cache::makePermanent(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    assert( pageIt != _cachedPages.end() );

    pageIt->second.permanent = true;
}


void pages_cache::flush()
{
    for (auto cachedPage : _cachedPages) {
        if (cachedPage.second.dirty) {
            _pageWriter(cachedPage.second.page);
        }
    }
}


void pages_cache::_writeAndDestroyPage(const cached_page_info &pageInfo)
{
    if (pageInfo.dirty) {
        _pageWriter(pageInfo.page);
    }

    delete pageInfo.page;
}


void pages_cache::clearCache()
{
    for (auto cachedPage : _cachedPages) {
        _writeAndDestroyPage(cachedPage.second);
    }

    _cachedPages.clear();
}


pages_cache::~pages_cache()
{
    assert( _cachedPages.empty() );
    //clearCache();
}


void pages_cache::_evict()
{
    int pageId = _lruQueue.front();
    auto pageIt = _cachedPages.find(pageId);
    assert( pageIt != _cachedPages.end() );

    cached_page_info &pageInfo = pageIt->second;
    if (pageInfo.pinned > 0 || pageInfo.permanent) {
        _statistics.failedEvictions++;
        return;
    }

    _writeAndDestroyPage(pageInfo);
    _lruQueue.pop_front();
    _cachedPages.erase(pageIt);
}


void pages_cache::_lruAdd(cached_page_info &pageInfo)
{
    if (_lruQueue.size() >= _sizePages) {
        _statistics.ecivtionsCount++;
        _evict();
    }

    _lruQueue.push_back(pageInfo.page->id());
    pageInfo.lruQueueIterator = _lruQueue.cend();
    std::advance(pageInfo.lruQueueIterator, -1);
}


void pages_cache::_lruAccess(cached_page_info &pageInfo)
{
    _lruQueue.erase(pageInfo.lruQueueIterator);

    _lruQueue.push_back(pageInfo.page->id());
    pageInfo.lruQueueIterator = _lruQueue.cend();
    std::advance(pageInfo.lruQueueIterator, -1);
}


void pages_cache::makeEvictable(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    assert( pageIt != _cachedPages.end() );

    pageIt->second.permanent = false;
}


void pages_cache::pin(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    assert( pageIt != _cachedPages.end() );

    pageIt->second.pinned = 1;
}


void pages_cache::unpinIfClean(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    if (pageIt == _cachedPages.end()) return;
    //assert( pageIt != _cachedPages.end() );

    if (!pageIt->second.dirty) {
        pageIt->second.pinned = 0;
    }
}
