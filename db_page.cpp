
#include "db_page.hpp"

//----------------------------------------------------------------------------------------------------------------------

db_page::db_page (size_t index) : _index (index)
{

}


size_t db_page::index() const
{
    return _index;
}
