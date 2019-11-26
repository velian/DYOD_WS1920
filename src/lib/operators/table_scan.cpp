#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "../storage/table.hpp"
#include "resolve_type.hpp"
#include "table_scan.hpp"

namespace opossum {


TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type, const AllTypeVariant search_value)
  : AbstractOperator(in, nullptr), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const {
  return _column_id;
}

ScanType TableScan::scan_type() const {
  return _scan_type;
}

const AllTypeVariant& TableScan::search_value() const {
  return _search_value;

}

auto get_comparator(ScanType scanType) {
  return [scanType](const auto left, const auto right) {
    switch (scanType) {
      case ScanType::OpEquals: {
        return left == right;
      }
      case ScanType::OpNotEquals: {
        return left != right;
      }
      case ScanType::OpLessThan: {
        return left < right;
      }
      case ScanType::OpLessThanEquals: {
        return left <= right;
      }
      case ScanType::OpGreaterThan: {
        return left > right;
      }
      case ScanType::OpGreaterThanEquals: {
        return left >= right;
      }
     //bdefault: break;
    }
  };
}

/*
auto get_comparator(ScanType scanType) {
  switch (scanType) {
    case ScanType::OpEquals: {
      return [](const auto left, const auto right) -> bool { return left == right; };
      break;
    }
    case ScanType::OpNotEquals: {
      return [](const auto left, const auto right) -> bool { return left != right; };
      break;
    }
    case ScanType::OpLessThan: {
      return [](const auto left, const auto right) -> bool { return left < right; };
      break;
    }
    case ScanType::OpLessThanEquals: {
      return [](const auto left, const auto right) -> bool { return left <= right; };
      break;
    }
    case ScanType::OpGreaterThan: {
      return [](const auto left, const auto right) -> bool { return left > right; };
      break;
    }
    case ScanType::OpGreaterThanEquals: {
      return [](const auto left, const auto right) -> bool { return left >= right; };
      break;
    }
   //bdefault: break;
  }
};
*/


std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto table = _input_table_left();
  auto comparator = get_comparator(_scan_type);
  //for (auto idx = 0; idx < value_segment.size(); ++idx) {
    //  return comparator(value_segment.get(idx), search_value());
  //}

  for (int i = 0; i < table->chunk_count; i++) {
    const auto& chunk = table->get_chunk(ChunkID(i));
    const auto& segment = chunk.get_segment(_column_id);

    // Kann man den Typen direkt abfragen, dass man hier keinen vector
    // durchgehen muss?

    std::vector<std::string> possible_types{"int", "long", "float", "double", "string"};
    for(auto data_type : possible_types) {
      resolve_data_type(data_type, [&] (auto type) {
        using Type = typename decltype(type)::type;
        const auto value_segment = std::dynamic_pointer_cast<ValueSegment>(segment);
        if(value_segment != nullptr) {
          const auto values = value_segment->values();
          for (auto& value : values) {
            //if ()
          }
        }
        return;
      });
    }
    
  }

  
  return std::shared_ptr<const Table>();
}

} //namespace opossum
