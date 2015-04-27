
#include "pages_cache.h"
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

pages_cache::pages_cache(size_t sizePages) : _sizePages(sizePages)
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
    _cachedPages[page->id()] = cached_page_info(page);
}


void pages_cache::unpin(db_page *page)
{
    auto pageIt = _cachedPages.find(page->id());
    if (pageIt == _cachedPages.end()) return;

    pageIt->second.pinned = false;
}
