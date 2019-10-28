#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) {
  _segments.push_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
	Assert(values.size() == _segments.size(), "Incorrect Data rows vs segment Rows");
	for (size_t curSeg = 0; curSeg < _segments.size(); curSeg++){
		_segments[curSeg]->append(values[curSeg]);
	}
  
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  // Shouldn't this be segment ID? 
  return _segments[column_id];
}

uint16_t Chunk::column_count() const {
  return _segments.size();
}

uint32_t Chunk::size() const {
	if (column_count() > 0){
  		return _segments.front()->size();
  	}
  	else{
  	return 0;
  	}
}

}  // namespace opossum
