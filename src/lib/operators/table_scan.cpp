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
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"

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

std::shared_ptr<const Table> TableScan::_on_execute() {

  const auto in_table = _input_table_left();
  auto out_table = std::make_shared<Table>();
  //auto posList = std::make_shared<PosList>();
  auto posList = PosList();
  
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
    }
    return left > right;
  };

  for (ChunkID i = ChunkID(0); i < in_table->chunk_count(); i++) {
    const auto& chunk = in_table->get_chunk(i);
    const auto& segment = chunk.get_segment(_column_id);

    auto data_type = in_table->column_type(_column_id);
    
    resolve_data_type(data_type, [&] (auto type) {
      using Type = typename decltype(type)::type;

      const auto typed_value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
      if(typed_value_segment != nullptr) {

          auto values = typed_value_segment->values();
          for (size_t index = 0; index < values.size(); index++){
            auto value = values.at(index);
            if(
              comparator(
                scan_type(),
                value,
                type_cast<Type>(search_value())
              )
            ) {
              posList.emplace_back(RowID{i, ChunkOffset(index)});
            };
          }
          return;
      }

      const auto typed_dict_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment);
      if(typed_dict_segment != nullptr) {
        auto attribute_vector = typed_dict_segment->attribute_vector();
        ValueID size = static_cast<ValueID>(attribute_vector->size());

        switch(scan_type()) {
          case ScanType::OpEquals: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute == value){
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            };
            break;
          }
          case ScanType::OpNotEquals: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute != value){
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            };
            break;
          }
          case ScanType::OpLessThan: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute < value){
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            };
            break;
          }
          case ScanType::OpLessThanEquals: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute <= value){
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            };
            break;
          }
          case ScanType::OpGreaterThan: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute > value){
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            };
            break;
          }
          case ScanType::OpGreaterThanEquals: {
            auto value = typed_dict_segment->find_in_dict(search_value());
            for (auto index = ValueID(0); index < size; index++) {
              auto attribute = typed_dict_segment->attribute_vector()->get(i);
              if (attribute >= value){
                posList.emplace_back(RowID({i, ChunkOffset(index)}));
              }
            };
            break;
          }
        }

      const auto typed_ref_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment);
      if(typed_ref_segment != nullptr) {
        auto pos_list_size = typed_ref_segment->pos_list()->size();

        for(size_t index = 0; index < pos_list_size; index++) {
          auto row_id = typed_ref_segment->pos_list()->at(index);
          auto value = typed_ref_segment
            ->referenced_table()
            ->get_chunk(row_id.chunk_id)
            .get_segment(typed_ref_segment->referenced_column_id())
            ->operator[](row_id.chunk_offset);

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

  auto& chunk = out_table->get_chunk(ChunkID{0});

  for (auto column = ColumnID(0); column < in_table->column_count(); column++) {
    out_table->add_column_definition(in_table->column_name(column), in_table->column_type(column));
  }

  for (auto column = ColumnID(0); column < _input_table_left()->column_count(); column++) {
    const auto shared_segment = std::make_shared<ReferenceSegment>(in_table, column, std::make_shared<PosList>(posList));
    chunk.add_segment(shared_segment);
  }
  return out_table;
}

} //namespace opossum
