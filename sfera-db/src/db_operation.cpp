
#include "db_operation.h"

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

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
