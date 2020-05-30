// Copyright 2020 <telsamar>
#include <DBHashCreator.hpp>
#include <constants.hpp>
#include <logs.hpp>

FHandlerContainer DBHashCreator::openDB
        (const FDescriptorContainer &descriptors) {
    FHandlerContainer handlers;
    std::vector < rocksdb::ColumnFamilyHandle * > newHandles;
    rocksdb::DB *dbStrPtr;

    rocksdb::Status status =
            rocksdb::DB::Open(
                    options,
                    _path,
                    descriptors,
                    &newHandles,
                    &dbStrPtr);
    if (!status.ok()) std::cerr << status.ToString() << std::endl;
    status = dbStrPtr->Put(rocksdb::WriteOptions(), "key1", "value1");
    status = dbStrPtr->Put(rocksdb::WriteOptions(), "key2", "value2");
  status = dbStrPtr->Put(rocksdb::WriteOptions(), "key3", "value3");
    _db.reset(dbStrPtr);

    for (rocksdb::ColumnFamilyHandle *ptr : newHandles) {
        handlers.emplace_back(ptr);
    }

    return handlers;
}

FDescriptorContainer DBHashCreator::getFamilyDescriptors() {
    std::vector <std::string> family;
    FDescriptorContainer descriptors;
    rocksdb::Status status =
            rocksdb::DB::ListColumnFamilies(options,
                                            _path,
                                            &family);
  if (!status.ok()) std::cerr << status.ToString() << std::endl;

    for (const std::string &familyName : family) {
        descriptors.emplace_back(familyName,
                                 rocksdb::ColumnFamilyOptions());
    }
    return descriptors;
}

StrContainer DBHashCreator::getStrs(rocksdb::ColumnFamilyHandle *family) {
    boost::unordered_map <std::string, std::string> dbCase;
    std::unique_ptr <rocksdb::Iterator>
            it(_db->NewIterator(rocksdb::ReadOptions(), family));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();
        dbCase[key] = value;
    }
    return dbCase;
}

void DBHashCreator::getHash
        (rocksdb::ColumnFamilyHandle *family, StrContainer strContainer) {
  std::string value;
    for (auto it = strContainer.begin(); it != strContainer.end(); ++it) {
        std::string hash = picosha2::hash256_hex_string(it->first + it->second);
        //logs::logInfo(it->first, hash);
        rocksdb::Status status = _db->Put(rocksdb::WriteOptions(),
                                          family,
                                          it->first,
                                          hash);
      status = _db->Get(rocksdb::ReadOptions(), it->first, &value);
      std::cout << "key: " << it->first << " hash: " << hash << " = " << value
      << std::endl;
      if (!status.ok()) std::cerr << status.ToString() << std::endl;
    }
}

void DBHashCreator::startHash
        (FHandlerContainer *handlers,
                std::list <StrContainer> *StrContainerList) {
    while (!handlers->empty()) {
        _mutex.lock();
        if (handlers->empty()) {
            _mutex.unlock();
            continue;
        }
        auto &family = handlers->front();
        handlers->pop_front();

        StrContainer strContainer = StrContainerList->front();
        StrContainerList->pop_front();
        _mutex.unlock();
        getHash(family.get(), strContainer);
    }
}

void DBHashCreator::startThreads() {
    auto deskriptors = getFamilyDescriptors();
    auto handlers = openDB(deskriptors);

    std::list <StrContainer> StrContainerList;

    for (auto &family : handlers) {
        StrContainerList.push_back(
                getStrs(family.get()));
    }

    std::vector <std::thread> threads;
    threads.reserve(_threadCountHash);
    for (size_t i = 0; i < _threadCountHash; ++i) {
        threads.emplace_back(std::thread
                                     (&DBHashCreator::startHash,
                                      this,
                                      &handlers,
                                      &StrContainerList));
    }
    for (auto &th : threads) {
        th.join();
    }
}
