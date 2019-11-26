#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "../storage/table.hpp"
#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/dictionary_segment.hpp"
#include "table_scan.hpp"
#include "types.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                     const AllTypeVariant search_value)
    : AbstractOperator(in, nullptr), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

/* auto get_comparator(ScanType scanType) {
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
}*/

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
  // bin mir nicht sicher, wie wir das Lambda richtig ist, aber das baut zumindest grade
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
        // default: break;
    }
    return left > right;
  };

  for (ChunkID i = ChunkID(0); i < table->chunk_count(); i++) {
    const auto& chunk = table->get_chunk(i);
    const auto& segment = chunk.get_segment(_column_id);
    PosList posList = PosList();

    // The value vector of a ValueSegment is templated to match the
    // data type stored in the column.
    // That's why we use the resolve_data_type lambda expression
    // to create a typed_segment that we can handle in the
    // not templated TableScan operator. (first approach in the pdf)

    // über ColumID bei segment nachgucken, welchen Typ
    auto data_type = table->column_type(_column_id);

    // std::vector<std::string> possible_types{"int", "long", "float", "double", "string"};
    // for(auto data_type : possible_types) {
    resolve_data_type(data_type, [&](auto type) {
      using Type = typename decltype(type)::type;

      // is given segment a value segment?
      const auto typed_value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
      if (typed_value_segment != nullptr) {
        // for (auto idx = size_t(0); idx < typed_value_segment->size(); idx++) {
        auto values = typed_value_segment->values();
        for (size_t index = 0; index < values.size(); index++) {
          auto value = values.at(index);
          if (comparator(scan_type(), value, type_cast<Type>(search_value()))) {
            posList.emplace_back(RowID{i, ChunkOffset(index)});
          }
        }
        return;
      }

      const auto typed_dict_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
      if (typed_dict_segment != nullptr) {
        auto attribute_vector = typed_dict_segment->attribute_vector();
        ValueID size = static_cast<ValueID>(attribute_vector->size());

        switch (scan_type()) {
          case ScanType::OpEquals: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute == value) {
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            }
            break;
          }
          case ScanType::OpNotEquals: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute != value) {
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            }
            break;
          }
          case ScanType::OpLessThan: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute < value) {
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            }
            break;
          }
          case ScanType::OpLessThanEquals: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute <= value) {
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            }
            break;
          }
          case ScanType::OpGreaterThan: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute > value) {
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            }
            break;
          }
          case ScanType::OpGreaterThanEquals: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute >= value) {
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            }
            break;
          }
        }

      const auto typed_ref_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
      if(typed_ref_segment != nullptr) {
        auto pos_list_size = typed_ref_segment->pos_list()->size();

        for(size_t id = 0; id < pos_list_size; id++) {
          auto row_id = typed_ref_segment->pos_list()->at(id);
          auto value = typed_ref_segment->referenced_table()->get_value(
            row_id,
            typed_ref_segment->referenced_column_id()
          );

          if(
              comparator(
                scan_type(),
                type_cast<Type>(value),
                type_cast<Type>(search_value())
              )
            ) {
              posList.emplace_back(RowID{i, ChunkOffset(index)});
            };
        }
      }
      
      }
    });
  }

  return std::shared_ptr<const Table>();
}

}  //  namespace opossum
