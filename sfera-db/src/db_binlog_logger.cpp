
#include "db_binlog_logger.hpp"
#include "db_stable_storage_file.hpp"

#include <iostream>
#include <cassert>

//----------------------------------------------------------------------------------------------------------------------

namespace sfera_db
{
//----------------------------------------------------------------------------------------------------------------------

binlog_record::binlog_record(type_t t, uint64_t lsn) :
        _type(t),
        _lsn(lsn)
{
    _length = _headerSize + sizeof(_length);
}


void binlog_record::_fillHeader(uint8_t *header)
{
    uint8_t *headerWriter = header;

    *(uint32_t *) headerWriter = _magic;
    headerWriter += sizeof(_magic);
    *(uint32_t *) headerWriter = _length;
    headerWriter += sizeof(_length);
    *(uint64_t *) headerWriter = _lsn;
    headerWriter += sizeof(_lsn);
    *(type_t *) headerWriter = _type;
    headerWriter += sizeof(_type);

    assert(headerWriter - header == _headerSize);
}


void binlog_record::_unpackHeader(uint8_t *header)
{
    uint8_t *headerReader = header;

    uint32_t magic = *(uint32_t *) headerReader;
    headerReader += sizeof(_magic);
    if (magic != _magic) {
        _type = UNKNOWN;
        return;
    }

    _length = *(uint32_t *) headerReader;
    headerReader += sizeof(_length);
    _lsn = *(uint64_t *) headerReader;
    headerReader += sizeof(_lsn);
    _type = *(type_t *) headerReader;
    headerReader += sizeof(_type);

    assert(headerReader - header == _headerSize);
}


void binlog_record::writeTo(raw_file *file)
{
    uint8_t header[_headerSize];
    _fillHeader(header);

    std::pair<const void *, size_t> buffers[] = {
            {header,   sizeof(header)},
            {&_length, sizeof(_length)}
    };

    file->appedAll(buffers, 2);
}


bool binlog_record::readFrom(raw_file *file)
{
    uint8_t header[_headerSize];
    file->readAll(header, _headerSize);
    _unpackHeader(header);

    if (_type == UNKNOWN) {
        assert(0);
        return false;
    }

    ::lseek(file->uinxFD(), sizeof(_length), SEEK_CUR);  // this is to read the whole record
    return true;
}


binlog_record::type_t
binlog_record::fetchType(raw_file *file)
{
    ::lseek(file->uinxFD(), _typeOffset(), SEEK_CUR);
    type_t recType;
    file->readAll(&recType, sizeof(recType));
    ::lseek(file->uinxFD(), -_typeOffset() - sizeof(recType), SEEK_CUR);

    return recType;
}

//----------------------------------------------------------------------------------------------------------------------

binlog_operation_record::binlog_operation_record(binlog_record::type_t t, uint64_t lsn, db_operation *op) :
        binlog_record(t, lsn),
        _operation(op)
{
    auto pagesCount = _operation->pagesWriteSet().size();
    _length += sizeof(uint32_t) /* pagesCount */ +
               (_operation->pagesWriteSet().begin()->second->size() + sizeof(int)) * pagesCount;
}


void binlog_operation_record::writeTo(raw_file *file)
{
    uint8_t header[_headerSize];
    _fillHeader(header);

    auto pages = _operation->pagesWriteSet();
    uint32_t pagesCount = (uint32_t) pages.size();
    uint32_t pagesIds[pagesCount];

    uint32_t buffersCount = (uint32_t) pages.size() + 4;
    auto buffers = new std::pair<const void *, size_t>[buffersCount];
    buffers[0] = {header, sizeof(header)};
    buffers[1] = {&pagesCount, sizeof(pagesCount)};
    buffers[2] = {pagesIds, sizeof(pagesIds)};

    auto pagesIt = pages.cbegin();
    for (int i = 0; i < pages.size(); ++i, ++pagesIt) {
        pagesIt->second->prepareForWriting();
        buffers[i + 3] = {pagesIt->second->bytes(), pagesIt->second->size()};
        pagesIds[i] = pagesIt->second->id();
    }
    buffers[buffersCount - 1] = {&_length, sizeof(_length)};

    file->appedAll(buffers, buffersCount);
    delete buffers;
}


bool binlog_operation_record::readFrom(raw_file *file)
{
    if (!binlog_record::readFrom(file)) return false;
    ::lseek(file->uinxFD(), -sizeof(_length), SEEK_CUR);  // this is to undo last length skipping

    uint32_t pageCount = 0;
    file->readAll(&pageCount, sizeof(pageCount));

    uint32_t pagesIds[pageCount];
    file->readAll(pagesIds, sizeof(pagesIds));

    uint32_t pageSize = (uint32_t) (_length - _headerSize - sizeof(_length) - sizeof(pageCount) - sizeof(pagesIds));
    pageSize /= pageCount;

    for (int i = 0; i < pageCount; ++i) {

        uint8_t *pageBytes = (uint8_t *) ::malloc(pageSize);
        file->readAll(pageBytes, pageSize);
        db_page *nextPage = db_page::load(pagesIds[i], data_blob(pageBytes, pageSize));
        _operation->writesPage(nextPage);
    }

    ::lseek(file->uinxFD(), sizeof(_length), SEEK_CUR);  // skip the length finally
    return true;
}

//----------------------------------------------------------------------------------------------------------------------

db_binlog_logger *db_binlog_logger::createEmpty(const std::string &path)
{
    db_binlog_logger *binlog = new db_binlog_logger();
    binlog->_file = raw_file::createNew(path);
    binlog->logCheckpoint();

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

    _file = raw_file::openExisting(path, true);
    ::lseek(_file->uinxFD(), -sizeof(uint32_t), SEEK_END);
    uint32_t lastMsgLen = 0;
    _file->readAll(&lastMsgLen, sizeof(lastMsgLen));
    ::lseek(_file->uinxFD(), -((off_t) lastMsgLen), SEEK_CUR);

    binlog_record lastRecord;
    if (!lastRecord.readFrom(_file)) {
        return;
    }

    if (lastRecord.type() == binlog_record::LOG_CLOSED) {
        _closedProperly = true;
    }
}


db_binlog_recovery::~db_binlog_recovery()
{
    if (_file != nullptr) {
        delete _file;
    }
}


void db_binlog_recovery::doRecovery(db_stable_storage_file *stableStorage)
{
    _findBackCheckpoint();

    while (!_file->eof()) {
        binlog_record::type_t nextRecordType = binlog_record::fetchType(_file);
        if (_file->eof()) break;
        if (nextRecordType == binlog_record::LOG_CLOSED) break;
        assert(nextRecordType == binlog_record::OPERATION);

        db_operation nextOperation(0);
        binlog_operation_record nextOperationRec(&nextOperation);
        nextOperationRec.readFrom(_file);

        auto pagesSet = nextOperation.pagesWriteSet();
        for (auto nextPagePair : pagesSet) {
            db_page *nextPage = nextPagePair.second;
            db_page *stablePageVersion = stableStorage->loadPage(nextPage->id());

            if (stablePageVersion == nullptr ||
                stablePageVersion->lastModifiedOpId() < nextPage->lastModifiedOpId()) {

                std::cerr << "[recovery] page #" << nextPage->id() << " fixed from " <<
                (stablePageVersion == nullptr ? 0 : stablePageVersion->lastModifiedOpId()) <<
                " to " << nextPage->lastModifiedOpId() << std::endl;

                stableStorage->writePage(nextPage);  // replace the page in stable storage
            }

            delete nextPage;
            delete stablePageVersion;
        }
    }
}


void db_binlog_recovery::_findBackCheckpoint()
{
    ::lseek(_file->uinxFD(), 0, SEEK_END);

    while (::lseek(_file->uinxFD(), -sizeof(uint32_t), SEEK_CUR) != -1) {
        uint32_t msgLen = 0;
        _file->readAll(&msgLen, sizeof(msgLen));
        off_t msgBegin = ::lseek(_file->uinxFD(), -(off_t) msgLen, SEEK_CUR);

        binlog_record record;
        if (!record.readFrom(_file)) {
            assert(0); // TODO: handle invalid log format
            return;
        }

        if (record.type() == binlog_record::CHECKPOINT) {
            break;
        }

        ::lseek(_file->uinxFD(), msgBegin, SEEK_SET);
    }
}

//----------------------------------------------------------------------------------------------------------------------
}
