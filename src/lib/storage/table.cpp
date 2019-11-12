#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>
#include <thread>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "dictionary_segment.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _chunk_size(chunk_size) { _add_chunk(); }

void Table::add_column(const std::string& name, const std::string& type) {
  Assert(_chunks.front()->size() == 0, "Data is present, cannot add Column");

  _column_names.push_back(name);
  _column_types.push_back(type);

  _chunks.front()->add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_chunks.back()->size() == _chunk_size) {
    _add_chunk();
    for (size_t i = 0; i < _column_types.size(); i++) {
      _chunks.back()->add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(_column_types[i]));
    }
  }
  _chunks.back()->append(values);
}

uint16_t Table::column_count() const {
  // Every Column should have a name
  return _column_names.size();
}

uint64_t Table::row_count() const { return ((_chunks.size() - 1) * _chunk_size + _chunks.back()->size()); }

ChunkID Table::chunk_count() const { return ChunkID{ static_cast<uint32_t>(_chunks.size())}; }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  // todo : Verify what instrucotr meant with C++20 solution: (unordered_)map::contains
  Assert(find(_column_names.begin(), _column_names.end(), column_name) != _column_names.end(), "Column Name Incorrect");
  return ColumnID{static_cast<uint16_t>(std::distance(_column_names.begin(), std::find(_column_names.begin(), _column_names.end(), column_name)))};
}

uint32_t Table::max_chunk_size() const { return _chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) { return *_chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return *_chunks.at(chunk_id); }

void Table::emplace_chunk(Chunk chunk) {
  // todo: This methid does not work as intended it clears out everything instead of emplacing
  //  find out what happens if the last chunk in the table is non empty
  // _chunks.clear();
  // std::shared_ptr<Chunk> new_chunk = std::make_shared<Chunk>(std::move(chunk));
  // _chunks.push_back(new_chunk);
}

void Table::_add_chunk() { _chunks.push_back(std::make_shared<Chunk>()); }

void Table::compress_segment(std::shared_ptr<BaseSegment> value_segment, std::shared_ptr<Chunk> compressed_chunk) {
  auto dictionary_segment = make_shared_by_data_type<BaseSegment, DictionarySegment>(column_type(segment_ID), value_segment);
  compressed_chunk->add_segment(dictionary_segment);
}

void Table::compress_chunk(ChunkID chunk_id) {
  const auto& uncompressed_chunk = get_chunk(chunk_id);
  std::shared_ptr<Chunk> compressed_chunk = std::make_shared<Chunk>();
  std::vector<std::thread> thread_vector;

  uint32_t number_of_segments = uncompressed_chunk.size();
  for (ColumnID segment_ID = ColumnID{0}; segment_ID < number_of_segments; segment_ID++) {
    std::shared_ptr<BaseSegment> value_segment = uncompressed_chunk.get_segment(segment_ID);
    std::thread segment_thread (compress_segment, value_segment, compressed_chunk);
    thread_vector.push_back(segment_thread);
  }
  _chunks[chunk_id] = compressed_chunk;
 }

}  // namespace opossum
