
#ifndef SFERA_DB_DB_BINLOG_H
#define SFERA_DB_DB_BINLOG_H

//----------------------------------------------------------------------------------------------------------------------

#include "raw_file.hpp"
#include "db_operation.h"

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{

    class binlog_record
    {
    public:
        enum type_t : uint8_t { UNKNOWN, OPERATION, LOG_CLOSED, CHECKPOINT };

    protected:
        type_t _type   = UNKNOWN;
        size_t _length = 0;
        uint64_t _lsn  = 0;

        const uint64_t _magic = 0x109be912219eb901;
        const size_t _headerSize = sizeof(_magic) + sizeof(_length) + sizeof(_lsn) + sizeof(_type);


    protected:
        void _fillHeader(uint8_t *header);
        void _unpackHeader(uint8_t *header);

    public:
        binlog_record(type_t t, uint64_t lsn);
        binlog_record() { };

        virtual void writeTo(raw_file *file);
        virtual bool readFrom(raw_file *file);

        inline type_t   type() const        { return _type; }
        inline size_t   length() const      { return _length; }
        inline uint64_t lsn() const         { return _lsn; }
    };

    //----------------------------------------------------------------------------------------------------------------------

    class binlog_operation_record : public binlog_record
    {
    protected:
        db_operation *_operation;

    public:
        binlog_operation_record(type_t t, uint64_t lsn, db_operation *op);

        virtual void writeTo(raw_file *file);
        virtual bool readFrom(raw_file *file);
    };

    //----------------------------------------------------------------------------------------------------------------------

    class db_binlog
    {
    public:

    private:
        raw_file *_file = nullptr;
        uint64_t  _currentLSN = 0;

    private:
        void _writeNextRecord(binlog_record &rec);

    private:
        db_binlog() { };

    public:
        ~db_binlog();
        static db_binlog *createEmpty(const std::string& path);
        static db_binlog *openExisting(const std::string& path);

        void logOperation(db_operation *operation);
    };

}

//----------------------------------------------------------------------------------------------------------------------

#endif    //SFERA_DB_DB_BINLOG_H
