
#ifndef SFERA_DB_DB_OPERATION_H
#define SFERA_DB_DB_OPERATION_H

//----------------------------------------------------------------------------------------------------------------------

#include <stdint.h>
#include <unordered_map>

#include "db_page.hpp"

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    class db_operation
    {
    private:
        uint64_t _id;
        std::unordered_map<int, db_page*> _pagesWriteSet;

    public:
        db_operation(uint64_t id) : _id(id) { }

        void writesPage(db_page* page);
        void invalidatePage(int pageId);

        bool isReadOnly();

        inline uint64_t id() const  { return _id; }
        inline const std::unordered_map<int, db_page*>& pagesWriteSet()  { return _pagesWriteSet; }
    };

}


//----------------------------------------------------------------------------------------------------------------------

#endif    //SFERA_DB_DB_OPERATION_H
