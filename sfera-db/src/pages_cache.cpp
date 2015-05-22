
#include "pages_cache.hpp"
#include <cassert>
#include <iostream>

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

//    _writerThread = new std::thread([this]() { _writerThreadRoutine(); });
}


db_page* pages_cache::fetchAndPin(int pageId)
{
    _statistics.fetchesCount++;

    auto pageIt = _cachedPages.find(pageId);
    if (pageIt == _cachedPages.end()) {
        _statistics.missesCount++;
        return nullptr;
    }

    pin(pageIt->second);
    _lruAccess(pageIt->second);
    return pageIt->second;
}


void pages_cache::cacheAndPin(db_page *page)
{
    assert( page != nullptr );
    assert( _cachedPages.find(page->id()) == _cachedPages.end() );

    _cachedPages.emplace(page->id(), page);
    pin(page);
    _lruAdd(page);
}


void pages_cache::unpin(db_page *page)
{
    assert( page != nullptr );
    assert( _cachedPages.find(page->id()) != _cachedPages.end() );
    assert( page->cacheRelatedInfo().pinned > 0 );

    page->cacheRelatedInfo().pinned--;
}


void pages_cache::invalidateCachedPage(int pageId)
{
    auto pageIt = _cachedPages.find(pageId);
    assert( pageIt != _cachedPages.end() );

    auto pageCacneInfo = pageIt->second->cacheRelatedInfo();
    assert( !pageCacneInfo.isUsed() );

    _finalizePage(pageIt->second);
    _lruQueue.erase(pageCacneInfo.lruQueueIterator);
    _cachedPages.erase(pageId);
}


void pages_cache::makeDirty(db_page *page)
{
    assert( page != nullptr );
    assert( _cachedPages.find(page->id()) != _cachedPages.end() );

    auto& cachedPageInfo = page->cacheRelatedInfo();
    cachedPageInfo.dirty = true;
}


void pages_cache::flush()
{
    for (auto cachedPage : _cachedPages) {
        if (cachedPage.second->cacheRelatedInfo().dirty) {
            _pageWriter(cachedPage.second);
            cachedPage.second->cacheRelatedInfo().dirty = false;
        }
    }
}


void pages_cache::_finalizePage(db_page *page)
{
    if (page->cacheRelatedInfo().dirty) {
        _pageWriter(page);
    }

    assert( !page->cacheRelatedInfo().isUsed() );
    _removeFromCache(page);
    delete page;
}


void pages_cache::clearCache()
{
    std::vector<db_page *> pages;
    pages.reserve(_cachedPages.size());

    for (auto cachedPage : _cachedPages) {
        if (cachedPage.second->cacheRelatedInfo().dirty)
            pages.push_back(cachedPage.second);
    }

    _cachedPages.clear();
    _lruQueue.clear();

    for (auto page : pages) {
        _pageWriter(page);
    }
}


pages_cache::~pages_cache()
{
    /*
    _writerThreadWorking = false;
    _writeQueueCondVar.notify_all();
    _writerThread->join();
    */

    assert( _cachedPages.empty() );
    //clearCache();
}


bool pages_cache::_evict()
{
    _statistics.ecivtionsCount++;

    int pageId = _lruQueue.front();
    auto pageIt = _cachedPages.find(pageId);
    assert( pageIt != _cachedPages.end() );

    auto& pageInfo = pageIt->second->cacheRelatedInfo();
    if (pageInfo.isUsed()) {
        _statistics.failedEvictions++;
        return false;
    }

    _lruQueue.pop_front();

    //if (pageInfo.dirty) _enqueuePageWrite(pageInfo.page);
    _finalizePage(pageIt->second);
    return true;
}


void pages_cache::_removeFromCache(db_page *page)
{
    //_pagesCacheMutex.lock();
    _cachedPages.erase(page->id());
    //_pagesCacheMutex.unlock();
}


void pages_cache::_lruAdd(db_page *page)
{
    while (_lruQueue.size() >= _sizePages && _evict());

    _lruQueue.push_back(page->id());
    page->cacheRelatedInfo().lruQueueIterator = _lruQueue.cend();
    std::advance(page->cacheRelatedInfo().lruQueueIterator, -1);
}


void pages_cache::_lruAccess(db_page *page)
{
    _lruQueue.erase(page->cacheRelatedInfo().lruQueueIterator);

    _lruQueue.push_back(page->id());
    page->cacheRelatedInfo().lruQueueIterator = _lruQueue.cend();
    std::advance(page->cacheRelatedInfo().lruQueueIterator , -1);
}


void pages_cache::pin(db_page *page)
{
    assert( page != nullptr );
    assert( _cachedPages.find(page->id()) != _cachedPages.end() );

    auto& cachedPageInfo = page->cacheRelatedInfo();
    cachedPageInfo.pinned++;
}

/*
void pages_cache::_writerThreadRoutine()
{
    std::unique_lock<std::mutex> lock(_writeQueueMutex);
    while (_writerThreadWorking) {

        while (!_pageToWriteAvaliable) {
            _writeQueueCondVar.wait(lock);
            if (!_writerThreadWorking) break;
        }

        while (!_diskWriteQueue.empty()) {
            if (_diskWriteQueue.front()->cacheRelatedInfo()->pinned == 0) {
                //_pageWriter(_diskWriteQueue.front());
                std::cout << "writing: " << _diskWriteQueue.front()->id() << std::endl;
                _finalizePage(*_diskWriteQueue.front()->cacheRelatedInfo());
            }

            _diskWriteQueue.pop();
        }

        _pageToWriteAvaliable = false;
    }
}


void pages_cache::_enqueuePageWrite(db_page* page)
{
    std::unique_lock<std::mutex> lock(_writeQueueMutex);

    _diskWriteQueue.push(page);
    _pageToWriteAvaliable = true;
    _writeQueueCondVar.notify_one();
}
*/

//----------------------------------------------------------------------------------------------------------------------
}
