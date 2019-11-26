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

/*auto get_comparator(ScanType scanType) {
  switch (scanType) {
    case ScanType::OpEquals: {
      return [](auto left, auto right) { return left == right; };
      break;
    }
    case ScanType::OpNotEquals: {
      return [](auto left, auto right) { return left != right; };
      break;
    }
    case ScanType::OpLessThan: {
      return [](auto left, auto right) { return left < right; };
      break;
    }
    case ScanType::OpLessThanEquals: {
      return [](auto left, auto right) { return left <= right; };
      break;
    }
    case ScanType::OpGreaterThan: {
      return [](auto left, auto right) { return left > right; };
      break;
    }
    case ScanType::OpGreaterThanEquals: {
      return [](auto left, auto right) { return left >= right; };
      break;
    }
    //default: break;
  }
}*/

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto table = _input_table_left();
  //auto comparator = get_comparator(_scan_type);
  auto comparator = [](ScanType scanType, auto left, auto right) {
  switch (scanType) {
    case ScanType::OpEquals: {
        return left == right;
        break;
      }
      case ScanType::OpNotEquals: {
        return left != right;
        break;
      }
      case ScanType::OpLessThan: {
        return left < right;
        break;
      }
      case ScanType::OpLessThanEquals: {
        return left <= right;
        break;
      }
      case ScanType::OpGreaterThan: {
        return left > right;
        break;
      }
      case ScanType::OpGreaterThanEquals: {
        return left >= right;
        break;
      }
      //default: break;
    }
  };

  for (ChunkID i = ChunkID(0); i < table->chunk_count(); i++) {
    const auto& chunk = table->get_chunk(i);
    const auto& segment = chunk.get_segment(_column_id);

    // The value vector of a ValueSegment is templated to match the
    // data type stored in the column.
    // That's why we use the resolve_data_type lambda expression
    // to create a typed_segment that we can handle in the 
    // not templated TableScan operator. (first approach in the pdf)
    
    // über ColumID bei segment nachgucken, welchen Typ
    auto data_type = table->column_type(_column_id);
    
    //std::vector<std::string> possible_types{"int", "long", "float", "double", "string"};
    //for(auto data_type : possible_types) {
    resolve_data_type(data_type, [&] (auto type) {
      using Type = typename decltype(type)::type;

      // is given segment a value segment?
      const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
      if(typed_segment != nullptr) {
        PosList posList = PosList();

        bool in_scope;
        for (auto idx = size_t(0); idx < typed_segment->size(); idx++) {
          auto values = typed_segment->values();
          auto value = values.at(idx);
          in_scope = comparator(_scan_type, value, type_cast<Type>search_value);
        }
      }
    });
    //}
  }

  
  return std::shared_ptr<const Table>();
}

} //namespace opossum
