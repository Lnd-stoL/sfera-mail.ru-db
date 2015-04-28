
#include "pages_cache.h"
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
    auto pageIt = _cachedPages.find(pageId);
    if (pageIt == _cachedPages.end()) return nullptr;

    pageIt->second.pinned = true;
    return pageIt->second.page;
}


void pages_cache::cacheAndPin(db_page *page)
{
    assert( page != nullptr );
    auto pageIt = _cachedPages.find(page->id());

    if (pageIt == _cachedPages.end()) {
        _cachedPages[page->id()] = cached_page_info(page);
    }
}


void pages_cache::unpin(db_page *page)
{
    assert( page != nullptr );

    auto pageIt = _cachedPages.find(page->id());
    if (pageIt == _cachedPages.end()) return;

    pageIt->second.pinned = false;
}


void pages_cache::invalidateCachedPage(int pageId)
{
    db_page *page = _cachedPages[pageId].page;
    _cachedPages.erase(page->id());
    delete page;
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
