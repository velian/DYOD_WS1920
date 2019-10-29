#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(has_table(name) == false, "Table already exists");
  _tableMap[name] = table;
}

void StorageManager::drop_table(const std::string& name) {
  Assert(has_table(name), "Table cannot be dropped: It does not exist");
  _tableMap.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  Assert(has_table(name), "Table Cannot be retrieved: It does not exist");
  return _tableMap.at(name);
}

bool StorageManager::has_table(const std::string& name) const {
  return _tableMap.count(name);
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  for (auto const& name : _tableMap){
    names.push_back(name.first);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  for (auto const& tuple : _tableMap){
    out << tuple.first 
    + " #Columns : " + std::to_string(tuple.second->column_count())
    + " #Rows " + std::to_string(tuple.second->row_count())
    + "#Chunks" + std::to_string(tuple.second->chunk_count());
  }
}

void StorageManager::reset() {
  _tableMap.clear();
}

}  // namespace opossum
