
#include "db_operation.hpp"
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

bool db_operation::writesPage(db_page *page)
{
    auto pageIt = _pagesWriteSet.find(page->id());
    if (pageIt == _pagesWriteSet.end()) {
        _pagesWriteSet.insert(std::make_pair(page->id(), page));
        return false;
    }

    assert( pageIt->second == page );
    return true;
}


void db_operation::invalidatePage(int pageId)
{
    //auto pageIt = _pagesWriteSet.find(pageId);
    //if (pageIt == _pagesWriteSet.end()) return;

    _pagesWriteSet.erase(pageId);
}


bool db_operation::isReadOnly()
{
    return _pagesWriteSet.empty();
}

//----------------------------------------------------------------------------------------------------------------------
}