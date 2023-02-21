// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/commands/defrem.h"
#include "../../../../src/include/commands/explain.h"

#include "../../../../src/include/nodes/pathnodes.h"
#include "../../../../src/include/nodes/makefuncs.h"
#include "../../../../src/include/nodes/nodeFuncs.h"
#include "../../../../src/include/nodes/execnodes.h"
#include "../../../../src/include/nodes/extensible.h"

#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "../../../../src/include/foreign/foreign.h"

#include "../../../../src/include/miscadmin.h"

#include "../../../../src/include/access/sysattr.h"
#include "../../../../src/include/access/parallel.h"
#include "../../../../src/include/access/htup_details.h"
#include "../../../../src/include/access/nbtree.h"
#include "../../../../src/include/access/relation.h"

#include "../../../../src/include/catalog/pg_class.h"
#include "../../../../src/include/catalog/pg_foreign_table.h"
#include "../../../../src/include/catalog/pg_type.h"

#include "../../../../src/include/executor/spi.h"
#include "../../../../src/include/executor/tuptable.h"

#include "../../../../src/include/optimizer/cost.h"
#include "../../../../src/include/optimizer/clauses.h"
#include "../../../../src/include/optimizer/pathnode.h"
#include "../../../../src/include/optimizer/optimizer.h"
#include "../../../../src/include/optimizer/paramassign.h"
#include "../../../../src/include/optimizer/paths.h"
#include "../../../../src/include/optimizer/placeholder.h"
#include "../../../../src/include/optimizer/plancat.h"
#include "../../../../src/include/optimizer/planmain.h"
#include "../../../../src/include/optimizer/prep.h"
#include "../../../../src/include/optimizer/restrictinfo.h"
#include "../../../../src/include/optimizer/subselect.h"
#include "../../../../src/include/optimizer/tlist.h"

#include "../../../../src/include/parser/parse_clause.h"
#include "../../../../src/include/parser/parsetree.h"
#include "../../../../src/include/partitioning/partprune.h"
#include "../../../../src/include/utils/lsyscache.h"
}
// clang-format on

#include "json.hpp"
using JsonType = nlohmann::json;
#include <variant>
#include <cassert>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <fstream>
using namespace::std;

#include <iostream>

void debug(string str) {
  elog(LOG, str.c_str());
}

enum DataType {
  IntType,
  FloatType,
  StringType,
  NoneType,
};

DataType string_to_datatype(string enum_str) {
  if (enum_str == "int") return DataType::IntType;
  else if (enum_str == "float") return DataType::FloatType;
  else if (enum_str == "str") return DataType::StringType;
  cout << "Error: Got datatype " << enum_str << endl;
  debug("Error: Got datatype " + enum_str);
  return NoneType;
}

int data_type_to_size(DataType dt) {
  switch(dt) {
    case IntType: return 4;
    case FloatType: return 4;
    case StringType: return 32;
  }
}

struct db721_BlockMetadataInt {
  int max_val;
  int min_val;
  int num_val;
  db721_BlockMetadataInt(JsonType json_obj):
    max_val(json_obj["max"]),
    min_val(json_obj["min"]),
    num_val(json_obj["num"])
  {
  }

  void print() {
    cout << "MaxVal: " << this->max_val << endl;
    cout << "MinVal: " << this->min_val << endl;
    cout << "NumVal: " << this->num_val << endl;
  }
};

struct db721_BlockMetadataFloat {
  float max_val;
  float min_val;
  int num_val;
  db721_BlockMetadataFloat(JsonType json_obj):
    max_val(json_obj["max"]),
    min_val(json_obj["min"]),
    num_val(json_obj["num"])
  {
  }
  void print() {
    cout << "MaxVal: " << this->max_val << endl;
    cout << "MinVal: " << this->min_val << endl;
    cout << "NumVal: " << this->num_val << endl;
  }
};

struct db721_BlockMetadataString {
  string max_val;
  int max_val_len;
  string min_val;
  int min_val_len;
  int num_val;
  db721_BlockMetadataString(JsonType json_obj):
    max_val(json_obj["max"]),
    max_val_len(json_obj["max_len"]),
    min_val(json_obj["min"]),
    min_val_len(json_obj["min_len"]),
    num_val(json_obj["num"])
  {
  }
  void print() {
    cout << "MaxVal: " << this->max_val << endl;
    cout << "MaxValLen: " << this->max_val_len << endl;
    cout << "MinVal: " << this->min_val << endl;
    cout << "MinValLen: " << this->min_val_len << endl;
    cout << "NumVal: " << this->num_val << endl;
  }
};

using db721_BlockMetadata = std::variant<db721_BlockMetadataInt, db721_BlockMetadataFloat, db721_BlockMetadataString>;

struct db721_ColumnMetadata {
  vector<db721_BlockMetadata> block_list;
  int num_blocks = 0;
  int start_offset = 0;
  string data_type_str = "";
  DataType data_type = NoneType;
  int data_size = 32;

  db721_ColumnMetadata () {}

  db721_ColumnMetadata(JsonType json_obj):
    num_blocks(json_obj["num_blocks"]),
    start_offset(json_obj["start_offset"]),
    data_type_str(json_obj["type"]),
    data_type(string_to_datatype(json_obj["type"])),
    data_size(data_type_to_size(data_type))
  {
    auto& block_stats = json_obj["block_stats"];
    for(auto it = block_stats.begin(); it != block_stats.end(); ++it) {
      switch (data_type) {
        case IntType: { block_list.emplace_back(db721_BlockMetadataInt(it.value())); break; }
        case FloatType: { block_list.emplace_back(db721_BlockMetadataFloat(it.value())); break; }
        case StringType: { block_list.emplace_back(db721_BlockMetadataString(it.value())); break; }
        default: { cout << "Error: NoneType found" << endl; break;}
      }
    }
  }

  void print() {
    cout << "NumBlocks: " << this->num_blocks << endl;
    cout << "StartOffset: " << this->start_offset << endl;
    cout << "DataTypeEnum: " << this->data_type << endl;
    cout << "DataType: " << this->data_type_str << endl;
    for(auto& block_val: this->block_list) {
      switch (this->data_type) {
        case IntType: { std::get<db721_BlockMetadataInt>(block_val).print(); break; }
        case FloatType: { std::get<db721_BlockMetadataFloat>(block_val).print(); break; }
        case StringType: { std::get<db721_BlockMetadataString>(block_val).print(); break; }
        default: { cout << "Error: NoneType found" << endl; break;}
      }
      cout << "**************************************************" << endl;
    }
  }
};

struct db721_TableMetadata {
  string filename;
  string tablename;
  map<string, db721_ColumnMetadata> column_map;
  int max_val_per_block;
  int num_rows;

  db721_TableMetadata () {
  }

  db721_TableMetadata(JsonType json_obj):
    tablename(json_obj["Table"])
  {
    max_val_per_block = json_obj["Max Values Per Block"];
    auto& cols = json_obj["Columns"];
    for (auto it = cols.begin(); it != cols.end(); ++it) {
      string col_name = it.key();
      column_map.emplace(make_pair(col_name, it.value()));
    }
  }

  void print() {
    cout << "Filename: " << this->filename << endl;
    cout << "Tablename: " << this->tablename << endl;
    cout << "MaxValPerBlock: " << this->max_val_per_block << endl;
    for(auto& [col_name, col_val]: this->column_map) {
      cout << "Column: " << col_name << endl;
      col_val.print();
      cout << "--------------------------------------------------" << endl;
    }
  }
};

map<Oid, db721_TableMetadata> gMetadataMap;

struct db721_TableInfo {
  string filename;
  string tablename;
};

struct db721_ScanState {
  db721_TableMetadata* metadata;
  ifstream fs;
  int cur_row = 0;
  set<int> attrs_used;
  set<int> attrs_returned;

  db721_ScanState (db721_TableMetadata* metadata):
    metadata(metadata),
    fs(metadata->filename, ifstream::binary),
    cur_row(0) {}

  TupleTableSlot* next(TupleTableSlot* slot) {
    if (cur_row == metadata->num_rows) {
      return nullptr;
    }
    auto* tuple_desc = slot->tts_tupleDescriptor;
    for(int attr = 0; attr < tuple_desc->natts; ++attr) {
      AttrNumber attrnum = attr + 1 - FirstLowInvalidHeapAttributeNumber;
      slot->tts_isnull[attrnum] = true;
      auto col_name = string(NameStr(TupleDescAttr(tuple_desc, attr)->attname));
      if (attrs_used.find(attrnum) == attrs_used.end()) continue;
      debug("Fetching column " + col_name);
      auto& col_data = metadata->column_map[col_name];
      fs.seekg(col_data.start_offset + cur_row * col_data.data_size);
      switch (col_data.data_type) {
        case IntType: {
          int val = 0;
          fs.read((char*)&val, col_data.data_size);
          slot->tts_values[attr] = Int32GetDatum(val);
          break;
        }
        case FloatType: {
          float val = 0;
          fs.read((char*)&val, col_data.data_size);
          slot->tts_values[attr] = Float4GetDatum(val);
          break;
        }
        case StringType: {
          text* t = (text*) palloc(col_data.data_size+VARHDRSZ);
          char data[32] = {};
          fs.read(data, col_data.data_size);
          SET_VARSIZE(t, col_data.data_size+VARHDRSZ);
          memcpy(VARDATA(t), data, col_data.data_size);
          slot->tts_values[attr] = PointerGetDatum(t);
          break;
        }
      }
    }
    cur_row++;
    return slot;
  }
};

struct db721_QueryPlan {
  db721_TableMetadata* metadata;
  Bitmapset* attrs_returned;
  Bitmapset* attrs_used;

  db721_QueryPlan() {}
  db721_QueryPlan(db721_TableMetadata* metadata):
    metadata(metadata) {
    }
};

void initialize(Oid foreigntableid, db721_TableInfo &info) {
  int metadata_size = 0;
  ifstream fs (info.filename, ifstream::binary);
  // Get metadata size
  fs.seekg(-4, fs.end);
  fs.read((char*)(&metadata_size), 4);
  // Get metadata as json
  fs.seekg(-1*(metadata_size + 4), fs.end);
  char* metadata_str = new char[metadata_size + 1]();
  fs.read(metadata_str, metadata_size);
  // TODO: Fix so that both lines below become one
  gMetadataMap.emplace(foreigntableid, JsonType::parse(metadata_str));
  gMetadataMap[foreigntableid].filename = info.filename;
}

void get_foreign_table_info(Oid foreigntableid, db721_TableInfo &info) {
  ForeignTable* table = GetForeignTable(foreigntableid);
  auto opt_list = table->options;
  for(int i = 0; i < opt_list->length; i++) {
    ListCell *lc = &(opt_list->elements[i]);
    auto def = (DefElem*) lfirst(lc);
    if (def->defnamespace != nullptr ) {
      cout << "Found namespace " << string(def->defnamespace) << endl;
    }
    if (string(def->defname) == "filename") {
      info.filename = defGetString(def);
      cout << "Found filename " << info.filename << endl;
    } else if (string(def->defname) == "tablename") {
      info.tablename = defGetString(def);
      cout << "Found tablename " << info.tablename << endl;
    }
  }
}

pair<Cost, Cost> estimate_costs(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
  return make_pair(0.0, 0.0);
}

void get_relevant_attrs(RelOptInfo* baserel) {
    db721_QueryPlan* query_plan = (db721_QueryPlan*) baserel->fdw_private;
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &query_plan->attrs_used);
    ListCell *lc;
    foreach(lc, baserel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
        pull_varattnos((Node *) rinfo->clause, baserel->relid, &query_plan->attrs_used);
    }

    if (bms_is_empty(query_plan->attrs_used))
    {
        bms_free(query_plan->attrs_used);
        query_plan->attrs_used = bms_make_singleton(1 - FirstLowInvalidHeapAttributeNumber);
    }
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  db721_TableInfo info;
  get_foreign_table_info(foreigntableid, info);

  if (gMetadataMap.find(foreigntableid) == gMetadataMap.end()) {
    initialize(foreigntableid, info);
  }
  auto& metadata = gMetadataMap[foreigntableid];
  int* attr_widths = new int[metadata.column_map.size()]();
  int row_width = 0;
  for(auto& [col_name, col_data]: metadata.column_map) {
    row_width += col_data.data_size;
  }
  auto& col_data = (metadata.column_map.begin()->second);
  int num_rows = 0;
  for(auto& blk: col_data.block_list) {
    int cur_blk_rows = 0;
    switch (col_data.data_type) {
      case IntType: { cur_blk_rows = get<db721_BlockMetadataInt>(blk).num_val; break; }
      case FloatType: { cur_blk_rows = get<db721_BlockMetadataFloat>(blk).num_val; break; }
      case StringType: { cur_blk_rows = get<db721_BlockMetadataString>(blk).num_val; break; }
      default: { cout << "Error: NoneType found" << endl; break;}
    }
    num_rows += cur_blk_rows;
  }
  metadata.num_rows = num_rows;
  baserel->fdw_private = &metadata;

  // TODO : Account of the "WHERE" filters
  baserel->rows = num_rows;
  baserel->reltarget->width = row_width;
  baserel->tuples = num_rows;
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                    Oid foreigntableid) {
  debug("Getting paths!");
  Cost startup_cost = 0;
  Cost run_cost = 0;
  auto* metadata = (db721_TableMetadata*) baserel->fdw_private;
  auto* query_plan = (db721_QueryPlan*) palloc0(sizeof(db721_QueryPlan));
  query_plan->metadata = metadata;

  baserel->fdw_private = query_plan;
  get_relevant_attrs(baserel);

  tie(startup_cost, run_cost) = estimate_costs(root, baserel, foreigntableid);
  auto* foreign_path = (Path*) create_foreignscan_path(root,
                                                       baserel,
                                                       NULL,
                                                       baserel->rows,
                                                       startup_cost, startup_cost + run_cost,
                                                       NULL,
                                                       NULL,
                                                       NULL,
                                                       (List*) baserel->fdw_private);
  add_path(baserel, (Path*) foreign_path);
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses,
                   Plan *outer_plan) {
  // TODO(721): Write me!
  // extern ForeignScan *make_foreignscan(List *qptlist, List *qpqual,
  //                    Index scanrelid, List *fdw_exprs, List *fdw_private,
  //                    List *fdw_scan_tlist, List *fdw_recheck_quals,
  //                    Plan *outer_plan);
  debug("Beginning scan! again");
  scan_clauses = extract_actual_clauses(scan_clauses, false);

  auto* query_plan = (db721_QueryPlan*) baserel->fdw_private;
  auto* ss = new db721_ScanState(query_plan->metadata);
  AttrNumber attr = -1;
  while ((attr = bms_next_member(query_plan->attrs_used, attr)) >= 0) {
    ss->attrs_used.insert(attr);
  }
  baserel->fdw_private = ss;
  return make_foreignscan(tlist, scan_clauses, baserel->relid, nullptr, (List*)(baserel->fdw_private), nullptr, nullptr, outer_plan);
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  ForeignScan* plan = (ForeignScan*) node->ss.ps.plan;
  node->fdw_state = plan->fdw_private;
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  db721_ScanState* scan_state = reinterpret_cast<db721_ScanState*>(node->fdw_state);
  TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
  ExecClearTuple(slot);
  db721_ScanState* myss = (db721_ScanState*) node->fdw_state;
  if ((slot = myss->next(slot)) == nullptr) {
    return nullptr;
  } else {
    ExecStoreVirtualTuple(slot);
    return slot;
  }
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}
