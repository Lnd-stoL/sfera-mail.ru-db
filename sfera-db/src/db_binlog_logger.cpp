
#include "db_binlog_logger.hpp"

#include <assert.h>

//----------------------------------------------------------------------------------------------------------------------

using namespace sfera_db;

//----------------------------------------------------------------------------------------------------------------------

binlog_record::binlog_record(type_t t, uint64_t lsn) :
        _type(t),
        _lsn(lsn)
{
    _length = sizeof(_magic) + 2 * sizeof(_length) + sizeof(_lsn) + sizeof(_type);
}


void binlog_record::_fillHeader(uint8_t *header)
{
    uint8_t *headerWriter = header;

    *(uint64_t*) headerWriter = _magic;         headerWriter += sizeof(uint64_t);
    *(size_t*) headerWriter = _length;          headerWriter += sizeof(size_t);
    *(uint64_t*) headerWriter = _lsn;           headerWriter += sizeof(uint64_t);
    *(type_t*) headerWriter = _type;            headerWriter += sizeof(type_t);

    assert( headerWriter - header == _headerSize );
}


void binlog_record::_unpackHeader(uint8_t *header)
{
    uint8_t *headerReader = header;

    uint64_t magic = *(uint64_t*) headerReader;   headerReader += sizeof(uint64_t);
    if (magic != _magic) {
        _type = UNKNOWN;
        return;
    }

    _length         = *(size_t*)headerReader;     headerReader += sizeof(size_t);
    _lsn            = *(uint64_t*)headerReader;   headerReader += sizeof(uint64_t);
    _type           = *(type_t*)headerReader;     headerReader += sizeof(type_t);

    assert( headerReader - header == _headerSize );
}


void binlog_record::writeTo(raw_file *file)
{
    uint8_t header[_headerSize];
    _fillHeader(header);

    std::pair<const void*, size_t> buffers[] = {
            { header,   sizeof(header)  },
            { &_length, sizeof(_length) }
    };

    file->appedAll(buffers, 2);
}


bool binlog_record::readFrom(raw_file *file)
{
    uint8_t header[_headerSize];
    file->readAll(header, _headerSize);
    _unpackHeader(header);

    if (_type == UNKNOWN) {
        return false;
    }

    return true;
}

//----------------------------------------------------------------------------------------------------------------------

binlog_operation_record::binlog_operation_record(binlog_record::type_t t, uint64_t lsn, db_operation *op) :
    binlog_record(t, lsn),
    _operation(op)
{
    auto pagesCount = _operation->pagesWriteSet().size();
    _length += sizeof(uint32_t) /* pagesCount */ + sizeof(uint32_t) * 2 * pagesCount +
            _operation->pagesWriteSet().begin()->second->size() * pagesCount;
}


void binlog_operation_record::writeTo(raw_file *file)
{
    uint8_t header[_headerSize];
    _fillHeader(header);

    auto pages = _operation->pagesWriteSet();
    uint32_t pagesData[pages.size()*2];

    uint32_t buffersCount = (uint32_t) pages.size() + 3;
    auto buffers = new std::pair<const void*, size_t>[buffersCount];
    buffers[0] = { header,      sizeof(header)  };
    buffers[1] = { pagesData,   sizeof(pagesData) };

    auto pagesIt = pages.cbegin();
    for (int i = 0; i < pages.size(); ++i, ++pagesIt) {
        pagesData[i*2 + 0] = pagesIt->second->size();
        pagesData[i*2 + 1] = pagesIt->second->id();

        pagesIt->second->prepareForWriting();
        buffers[i + 2]     = { pagesIt->second->bytes(), pagesIt->second->size() };
    }
    buffers[buffersCount - 1] = { &_length, sizeof(_length) };

    file->appedAll(buffers, buffersCount);
}


bool binlog_operation_record::readFrom(raw_file *file)
{
    return binlog_record::readFrom(file);
}

//----------------------------------------------------------------------------------------------------------------------

db_binlog_logger *db_binlog_logger::createEmpty(const std::string &path)
{
    db_binlog_logger *binlog = new db_binlog_logger();
    binlog->_file = raw_file::createNew(path);

    return binlog;
}


db_binlog_logger *db_binlog_logger::openExisting(const std::string &path)
{
    db_binlog_logger *binlog = new db_binlog_logger();
    binlog->_file = raw_file::openExisting(path);

    return binlog;
}


void db_binlog_logger::_writeNextRecord(binlog_record &rec)
{
    rec.writeTo(_file);
    ++_currentLSN;
}


db_binlog_logger::~db_binlog_logger()
{
    binlog_record opClose(binlog_record::LOG_CLOSED, _currentLSN);
    _writeNextRecord(opClose);
}


void db_binlog_logger::logOperation(db_operation *operation)
{
    binlog_operation_record rec(binlog_record::OPERATION, _currentLSN, operation);
    _writeNextRecord(rec);
}


void db_binlog_logger::logCheckpoint()
{
    binlog_record checkpointRec(binlog_record::CHECKPOINT, _currentLSN);
    _writeNextRecord(checkpointRec);
}

//----------------------------------------------------------------------------------------------------------------------

db_binlog_recovery::db_binlog_recovery(const std::string &path)
{
    if (!raw_file::exists(path)) {
        _file = nullptr;
        return;
    }

    _file = raw_file::openExisting(path);
    ::lseek(_file->uinxFD(), -sizeof(size_t), SEEK_END);
    size_t lastMsgLen = 0;
    _file->readAll(&lastMsgLen, sizeof(size_t));
    ::lseek(_file->uinxFD(), -lastMsgLen, SEEK_END);

    binlog_record lastRecord;
    if (!lastRecord.readFrom(_file)) {
        return;
    }

    if (lastRecord.type() == binlog_record::LOG_CLOSED) {
        _closedProperly = true;
    }
}


void db_binlog_recovery::doRecovery(db_data_storage *storage)
{
    _findBackCheckpoint();

    while (!_file->eof()) {

    }
}


void db_binlog_recovery::_findBackCheckpoint()
{
    while (::lseek(_file->uinxFD(), -sizeof(size_t), SEEK_CUR) == 0) {
        size_t msgLen = 0;
        _file->readAll(&msgLen, sizeof(size_t));
        ::lseek(_file->uinxFD(), -msgLen, SEEK_CUR);

        binlog_record record;
        if (!record.readFrom(_file)) {
            return;
        }

        if (record.type() == binlog_record::CHECKPOINT) {
            break;
        }
    }
}
