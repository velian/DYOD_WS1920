#include "reference_segment.hpp"

namespace opossum {

// ReferenceSegment is a specific segment type that stores all its values as position list of a referenced segment
ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table> referenced_table, const ColumnID referenced_column_id,
                   const std::shared_ptr<const PosList> pos): _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos_list(pos) {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
	RowID row = (*_pos_list)[chunk_offset];
	auto& chunk = _referenced_table->get_chunk(row.chunk_id);
	auto column = chunk.get_segment(_referenced_column_id);
	auto result_value = (*column)[row.chunk_offset];
	return result_value;
};

size_t ReferenceSegment::size() const {
	return _pos_list->size();
};

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const { return _pos_list; };

const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const { return _referenced_table; };

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; };

}  // namespace opossum
