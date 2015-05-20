
#include "pages_cache.hpp"
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

pages_cache::pages_cache(size_t sizePages, std::function<void(db_page*)> pageWriter) :
    _sizePages(sizePages),
    _pageWriter(pageWriter)
{
    _cachedPages.max_load_factor(1.5);
    _cachedPages.reserve((size_t)((double)sizePages * 1.5));
}


db_page* pages_cache::fetchAndPin(int pageId)
{
    _statistics.fetchesCount++;

    auto pageIt = _cachedPages.find(pageId);
    if (pageIt == _cachedPages.end()) {
        _statistics.missesCount++;
        return nullptr;
    }

    cached_page_info& cachedPageInfo = pageIt->second;
    cachedPageInfo.pinned++;
    _lruAccess(cachedPageInfo);

    return cachedPageInfo.page;
}


void pages_cache::cacheAndPin(db_page *page)
{
    assert( page != nullptr );
    assert( _cachedPages.find(page->id()) == _cachedPages.end() );

    cached_page_info newPageInfo(page);
    newPageInfo.pinned = 1;
    _lruAdd(newPageInfo);
    page->wasCached(&(_cachedPages.emplace(page->id(), newPageInfo).first->second));
}


void pages_cache::unpin(db_page *page)
{
    assert( page != nullptr );
    assert( page->cacheRelatedInfo() != nullptr );

    //auto pageIt = _cachedPages.find(page->id());
    //if (pageIt == _cachedPages.end()) return;

    auto cachedPageInfo = page->cacheRelatedInfo();

    assert( cachedPageInfo->pinned > 0 );    // this is actual for one-threaded db
    cachedPageInfo->pinned--;
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
    assert( page->cacheRelatedInfo() != nullptr );
    auto cachedPageInfo = page->cacheRelatedInfo();

    //auto pageIt = _cachedPages.find(page->id());
    //assert( pageIt != _cachedPages.end() );

    cachedPageInfo->dirty = true;
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


bool pages_cache::_evict()
{
    _statistics.ecivtionsCount++;

    int pageId = _lruQueue.front();
    auto pageIt = _cachedPages.find(pageId);
    assert( pageIt != _cachedPages.end() );

    cached_page_info &pageInfo = pageIt->second;
    if (pageInfo.pinned > 0) {
        _statistics.failedEvictions++;
        return false;
    }

    _writeAndDestroyPage(pageInfo);
    _lruQueue.pop_front();
    _cachedPages.erase(pageIt);

    return true;
}


void pages_cache::_lruAdd(cached_page_info &pageInfo)
{
    while (_lruQueue.size() >= _sizePages && _evict());

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


void pages_cache::pin(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    assert( pageIt != _cachedPages.end() );

    pageIt->second.pinned++;
}


void pages_cache::unpinIfClean(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    if (pageIt == _cachedPages.end()) return;
    //assert( pageIt != _cachedPages.end() );

    if (!pageIt->second.dirty) {
        pageIt->second.pinned--;
    }
}

//----------------------------------------------------------------------------------------------------------------------
}
