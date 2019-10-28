#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size):_chunk_size(chunk_size){
  _add_chunk();
}

void Table::add_column(const std::string& name, const std::string& type) {
  Assert(_chunks.front()->size()==0, "Data is present, cannot add Column");

  _columnNames.push_back(name);
  _columnTypes.push_back(type);

  _chunks.front()->add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_chunks.back()->size() == _chunk_size){
    _add_chunk();
    for (size_t i = 0; i < _columnTypes.size(); i++){
      _chunks.back()->add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(_columnTypes[i]));
    }
  }
  _chunks.back()->append(values);
}

uint16_t Table::column_count() const {
  // Every Column should have a name
  return _columnNames.size();
}

uint64_t Table::row_count() const {
  return ((_chunks.size() - 1) * _chunk_size + _chunks.back()->size() );
}

ChunkID Table::chunk_count() const {
  return ChunkID{_chunks.size()};
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  // pretty ugly unsure how to make better
  Assert(find(_columnNames.begin(), _columnNames.end(), column_name) != _columnNames.end(), "Column Name Incorrect");
  return ColumnID{distance(_columnNames.begin(), find(_columnNames.begin(), _columnNames.end(), column_name))};
} 

uint32_t Table::max_chunk_size() const {
  return _chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  return _columnNames;
}

const std::string& Table::column_name(ColumnID column_id) const {
  return _columnNames[column_id];
}

const std::string& Table::column_type(ColumnID column_id) const {
  return _columnTypes[column_id];
}


Chunk& Table::get_chunk(ChunkID chunk_id) { 
  return *_chunks[chunk_id]; 
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const { 
  return *_chunks[chunk_id];
}

void Table::_add_chunk(){
  _chunks.push_back(std::make_shared<Chunk>());
}

}  // namespace opossum
