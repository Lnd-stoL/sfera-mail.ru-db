
#include "db_operation.hpp"

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

void db_operation::writesPage(db_page *page)
{
    _pagesWriteSet[page->id()] = page;
}


void db_operation::invalidatePage(int pageId)
{
    auto pageIt = _pagesWriteSet.find(pageId);
    if (pageIt == _pagesWriteSet.end()) return;

    _pagesWriteSet.erase(pageIt);
}


bool db_operation::isReadOnly()
{
    return _pagesWriteSet.empty();
}

//----------------------------------------------------------------------------------------------------------------------
}