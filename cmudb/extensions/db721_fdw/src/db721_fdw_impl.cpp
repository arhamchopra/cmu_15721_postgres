// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"

#include "../../../../src/include/commands/defrem.h"

#include "../../../../src/include/foreign/fdwapi.h"
#include "../../../../src/include/foreign/foreign.h"

#include "../../../../src/include/access/nbtree.h"
#include "../../../../src/include/access/table.h"

#include "../../../../src/include/optimizer/pathnode.h"
#include "../../../../src/include/optimizer/optimizer.h"
#include "../../../../src/include/optimizer/planmain.h"
#include "../../../../src/include/optimizer/restrictinfo.h"

#include "../../../../src/include/utils/builtins.h"
#include "../../../../src/include/utils/lsyscache.h"
#include "../../../../src/include/utils/typcache.h"
}
// clang-format on

#include "json.hpp"
using JsonType = nlohmann::json;

#include <variant>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <fstream>
using namespace::std;

#include <iostream>

void debug(string str) {
  // cout << str << endl;
}

// Hardcoding the supported datatypes
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
  debug("Error: Got datatype " + enum_str);
  assert(false);
}

int data_type_to_size(DataType dt) {
  switch(dt) {
    case IntType: return 4;
    case FloatType: return 4;
    case StringType: return 32;
    default: {
               debug("Invalid Type");
               assert(false);
             }
  }
}

// Preprocess and create the FmgrInfo struct for use in comparision in the future
struct RowFilter {
  FmgrInfo finfo;
  Var* var;
  Const *const_val;
  int strategy;
  bool neg;

  RowFilter(Var* o_var, Const* o_const_val, int o_strategy, int cmp_proc_oid, bool o_neg):
    var(o_var),
    const_val(o_const_val),
    strategy(o_strategy),
    neg(o_neg) {
      fmgr_info(cmp_proc_oid, &finfo);
    }
};

// To store information about each of the blocks -- One for each of the datatypes
struct db721_BlockMetadataInt {
  int max_val = 0;
  int min_val = 0;
  int num_val = 0;
  db721_BlockMetadataInt() {}
  db721_BlockMetadataInt(JsonType &json_obj):
    max_val(json_obj["max"]),
    min_val(json_obj["min"]),
    num_val(json_obj["num"]) { }

  void print() {
    cout << "MaxVal: " << this->max_val << endl;
    cout << "MinVal: " << this->min_val << endl;
    cout << "NumVal: " << this->num_val << endl;
  }
};

struct db721_BlockMetadataFloat {
  float max_val = 0;
  float min_val = 0;
  int num_val = 0;
  db721_BlockMetadataFloat() {}
  db721_BlockMetadataFloat(JsonType &json_obj):
    max_val(json_obj["max"]),
    min_val(json_obj["min"]),
    num_val(json_obj["num"]) { }
  void print() {
    cout << "MaxVal: " << this->max_val << endl;
    cout << "MinVal: " << this->min_val << endl;
    cout << "NumVal: " << this->num_val << endl;
  }
};

struct db721_BlockMetadataString {
  string max_val;
  int max_val_len = 0;
  string min_val;
  int min_val_len = 0;
  int num_val = 0;
  db721_BlockMetadataString() {}
  db721_BlockMetadataString(JsonType &json_obj):
    max_val(json_obj["max"]),
    max_val_len(json_obj["max_len"]),
    min_val(json_obj["min"]),
    min_val_len(json_obj["min_len"]),
    num_val(json_obj["num"]) { }
  void print() {
    cout << "MaxVal: " << this->max_val << endl;
    cout << "MaxValLen: " << this->max_val_len << endl;
    cout << "MinVal: " << this->min_val << endl;
    cout << "MinValLen: " << this->min_val_len << endl;
    cout << "NumVal: " << this->num_val << endl;
  }
};

// Use variants to ease programming in the following code and hoping to not use void* with reinterpret_casting :p
using db721_BlockMetadata = std::variant<db721_BlockMetadataInt, db721_BlockMetadataFloat, db721_BlockMetadataString>;

// To keep track of all information about a column of the foreign data
struct db721_ColumnMetadata {
  // List of blocks
  vector<db721_BlockMetadata> block_list;
  // Starting offsets of the blocks
  vector<int> block_offset_list;
  // Keep track of the number of rows in each block could be replaced with a pair of integers since num_rows is the same for all blocks before the last one
  vector<int> row_count_list;
  // Total number of blocks -- Redundant
  int num_blocks;
  // Type of the data being stored
  DataType data_type;
  // Store the data size to avoid function call
  int data_size;
  // Global information across the blocks
  db721_BlockMetadata global_block;

  db721_ColumnMetadata():
    num_blocks(0),
    data_type(NoneType),
    data_size(0) { }

  db721_ColumnMetadata(JsonType &json_obj):
    num_blocks(json_obj["num_blocks"]),
    data_type(string_to_datatype(json_obj["type"])),
    data_size(data_type_to_size(data_type))
  {
    switch (data_type) {
      case IntType: {
                      global_block = db721_BlockMetadataInt();
                      break;
                    }
      case FloatType: {
                        global_block = db721_BlockMetadataFloat();
                        break;
                      }
      case StringType: {
                         global_block = db721_BlockMetadataString();
                         break;
                       }
      default: { cout << "Error: NoneType found" << endl; break;}
    }
    int start_offset = json_obj["start_offset"];
    int block_idx = 0;
    // Important to load data in right numerical order and json is sorted in lexical order
    for(block_idx = 0; block_idx < num_blocks; block_idx++) {
      auto& json_block = json_obj["block_stats"][to_string(block_idx)];
      switch (data_type) {
        case IntType: {
                        auto blk = db721_BlockMetadataInt(json_block);
                        block_list.emplace_back(blk);
                        block_offset_list.push_back(start_offset);
                        row_count_list.push_back(blk.num_val);
                        start_offset += blk.num_val * data_size;
                        auto& gblk = get<db721_BlockMetadataInt>(global_block);
                        gblk.max_val = max(gblk.max_val, blk.max_val);
                        gblk.min_val = min(gblk.min_val, blk.min_val);
                        break;
                      }
        case FloatType: {
                          auto blk = db721_BlockMetadataFloat(json_block);
                          block_list.emplace_back(blk);
                          block_offset_list.push_back(start_offset);
                          row_count_list.push_back(blk.num_val);
                          start_offset += blk.num_val * data_size;
                          auto& gblk = get<db721_BlockMetadataFloat>(global_block);
                          gblk.max_val = max(gblk.max_val, blk.max_val);
                          gblk.min_val = min(gblk.min_val, blk.min_val);
                          break;
                        }
        case StringType: {
                           auto blk = db721_BlockMetadataString(json_block);
                           block_list.emplace_back(blk);
                           block_offset_list.push_back(start_offset);
                           row_count_list.push_back(blk.num_val);
                           start_offset += blk.num_val * data_size;
                           auto& gblk = get<db721_BlockMetadataString>(global_block);
                           gblk.max_val = max(gblk.max_val, blk.max_val);
                           gblk.min_val = min(gblk.min_val, blk.min_val);
                           break;
                         }
        default: { cout << "Error: NoneType found" << endl; break;}
      }
    }
  }

  void print() {
    cout << "NumBlocks: " << this->num_blocks << endl;
    cout << "DataTypeEnum: " << this->data_type << endl;
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

// Check conditions for const value against the min and max values
bool check_conditions(FmgrInfo* finfo, Oid collid, int strategy, Datum const_val, Datum data_min, Datum data_max, bool neg) {
  int cmpres_min, cmpres_max;
  switch(strategy) {
    case BTLessStrategyNumber:
      {
        cmpres_min = FunctionCall2Coll(finfo, collid, const_val, data_min);
        return (cmpres_min > 0) ^ neg;
      }
    case BTLessEqualStrategyNumber:
      {
        cmpres_min = FunctionCall2Coll(finfo, collid, const_val, data_min);
        return (cmpres_min >= 0) ^ neg;
      }
    case BTGreaterStrategyNumber:
      {
        cmpres_max = FunctionCall2Coll(finfo, collid, const_val, data_max);
        return (cmpres_max < 0) ^ neg;
      }
    case BTGreaterEqualStrategyNumber:
      {
        cmpres_max = FunctionCall2Coll(finfo, collid, const_val, data_max);
        return (cmpres_max <= 0) ^ neg;
      }
    case BTEqualStrategyNumber:
      {
        cmpres_min = FunctionCall2Coll(finfo, collid, const_val, data_min);
        cmpres_max = FunctionCall2Coll(finfo, collid, const_val, data_max);
        // Special handling for neg
        if (neg) {
          return ((cmpres_min != 0) || (cmpres_max != 0));
        } else {
          return ((cmpres_min >= 0) && (cmpres_max <= 0));
        }
      }
    default: Assert(false);
  }
}

// Store all metadata information for a table, also stores the row filters for a query.
struct db721_TableMetadata {
  // These won't change as insert/updates are not supported
  string filename;
  vector<db721_ColumnMetadata> column_data;
  int max_val_per_block;
  int num_rows;
  int row_width;
  int num_blocks;

  // Should reset these at the start of each query
  vector<vector<RowFilter>> rfs;
  vector<int> relevant_blocks;

  db721_TableMetadata () {
  }

  db721_TableMetadata(JsonType &json_obj, TupleDescData *tuple_desc, string &filename):
    filename(filename) {
      max_val_per_block = json_obj["Max Values Per Block"];
      auto& cols = json_obj["Columns"];
      column_data.resize(tuple_desc->natts);
      for(int attrnum = 0; attrnum < tuple_desc->natts; attrnum++) {
        auto col_name = string(NameStr(TupleDescAttr(tuple_desc, attrnum)->attname));
        auto cols_it = cols.find(col_name);
        if (cols_it == cols.end()) {
          // No metadata found for this attribute
          debug("Skipping Varratno " + to_string(attrnum));
          // Should not happen
          continue;
        }
        column_data[attrnum] = db721_ColumnMetadata((*cols_it));
        row_width += column_data[attrnum].data_size;
      }
      num_rows = 0;
      if (column_data.size()) {
        num_blocks = column_data[0].num_blocks;
        for(auto& blk: column_data[0].block_list) {
          int cur_blk_rows = 0;
          switch (column_data[0].data_type) {
            case IntType: { cur_blk_rows = get<db721_BlockMetadataInt>(blk).num_val; break; }
            case FloatType: { cur_blk_rows = get<db721_BlockMetadataFloat>(blk).num_val; break; }
            case StringType: { cur_blk_rows = get<db721_BlockMetadataString>(blk).num_val; break; }
            default: { cout << "Error: NoneType found" << endl; break;}
          }
          num_rows += cur_blk_rows;
        }
      }
    }

  // Apply row filters to the block metadata and get only the useful blocks
  void extract_relevant_blocks() {
    vector<bool> useful_block(num_blocks, true);
    for(size_t attrnum = 0; attrnum < column_data.size(); attrnum++) {
      for(auto &rf: rfs[attrnum]) {
        auto &col_data = column_data[attrnum];

        Datum     const_val = rf.const_val->constvalue;
        int       collid = rf.const_val->constcollid;
        int       strategy = rf.strategy;
        FmgrInfo* finfo = &rf.finfo;
        bool      neg = rf.neg;

        Datum data_max;
        Datum data_min;
        // Check global information first
        switch (col_data.data_type) {
          case IntType:
            {
              auto& gblk = get<db721_BlockMetadataInt>(col_data.global_block);
              data_max = Int32GetDatum(gblk.max_val);
              data_min = Int32GetDatum(gblk.min_val);
              break;
            }
          case FloatType:
            {
              auto& gblk = get<db721_BlockMetadataFloat>(col_data.global_block);
              data_max = Float4GetDatum(gblk.max_val);
              data_min = Float4GetDatum(gblk.min_val);
              break;
            }
          case StringType:
            {
              auto& gblk = get<db721_BlockMetadataString>(col_data.global_block);
              data_max = CStringGetTextDatum(gblk.max_val.c_str());
              data_min = CStringGetTextDatum(gblk.min_val.c_str());
              break;
            }
          default: { cout << "Error: NoneType found" << endl; assert(false); break;}
        }
        if (not check_conditions(finfo, collid, strategy, const_val, data_min, data_max, neg)) {
          relevant_blocks.clear();
          return;
        }

        // Check individual blocks
        for(int block_idx = 0; block_idx < num_blocks; block_idx++) {
          if (not useful_block[block_idx]) continue;
          auto &blk = col_data.block_list[block_idx];
          switch (col_data.data_type) {
            case IntType:
              {
                data_max = Int32GetDatum(get<db721_BlockMetadataInt>(blk).max_val);
                data_min = Int32GetDatum(get<db721_BlockMetadataInt>(blk).min_val);
                break;
              }
            case FloatType:
              {
                data_max = Float4GetDatum(get<db721_BlockMetadataFloat>(blk).max_val);
                data_min = Float4GetDatum(get<db721_BlockMetadataFloat>(blk).min_val);
                break;
              }
            case StringType:
              {
                data_max = CStringGetTextDatum(get<db721_BlockMetadataString>(blk).max_val.c_str());
                data_min = CStringGetTextDatum(get<db721_BlockMetadataString>(blk).min_val.c_str());
                break;
              }
            default: { cout << "Error: NoneType found" << endl; break;}
          }
          useful_block[block_idx] = check_conditions(finfo, collid, strategy, const_val, data_min, data_max, neg);
        }
      }
    }
    // Store all the useful blocks
    for(int block_idx = 0; block_idx < num_blocks; block_idx++) {
      if (not useful_block[block_idx]) continue;
      relevant_blocks.push_back(block_idx);
    }
  }

  void print() {
    cout << "Filename: " << this->filename << endl;
    cout << "MaxValPerBlock: " << this->max_val_per_block << endl;
    for(auto& col_val: this->column_data) {
      col_val.print();
      cout << "--------------------------------------------------" << endl;
    }
  }
};

// Map to cache metadata and avoid loading multiple times
map<Oid, db721_TableMetadata> gMetadataMap;

// Extract datum from memory
void extract_next_datum(char* data_array, db721_ColumnMetadata &col_data, int idx, Datum &datum) {
  switch (col_data.data_type) {
    case IntType: {
                    int val = 0;
                    memcpy((char*)&val, (data_array + col_data.data_size * idx), col_data.data_size);
                    datum = Int32GetDatum(val);
                    break;
                  }
    case FloatType: {
                      float val = 0;
                      memcpy((char*)&val, (data_array + col_data.data_size * idx), col_data.data_size);
                      datum = Float4GetDatum(val);
                      break;
                    }
    case StringType: {
                       char val[33] = {};
                       memcpy(val, (data_array + col_data.data_size * idx), col_data.data_size);
                       datum = CStringGetTextDatum(val);
                       break;
                     }
    default: {
               debug("Invalid Type");
               assert(false);
             }
  }
}

struct db721_ScanState {
  db721_TableMetadata* metadata;
  // On filestream per column to avoid excessive seeks and maybe take advantage of locality
  vector<ifstream> fds;
  int cur_row_in_block = 0;
  int num_rows_in_block = 0;
  // Keep track of all relevant attributes
  vector<AttrNumber> v_attrs_used;
  vector<AttrNumber> v_attrs_returned;
  vector<AttrNumber> v_attrs_combined;
  // Load data for a block into memory for faster access assuming it can fit, should be okay otherwise need to implement paging of blocks in memory
  vector<char*> block_data;
  uint next_block_idx = 0;
  int num_blocks = 0;

  ~db721_ScanState() {
  }

  db721_ScanState (db721_TableMetadata* metadata):
    metadata(metadata),
    cur_row_in_block(0),
    num_rows_in_block(0),
    next_block_idx(0) {
      num_blocks = metadata->column_data[0].num_blocks;
      for(auto &col: metadata->column_data) {
        fds.emplace_back(metadata->filename, ifstream::binary);
        block_data.push_back((char*)palloc(metadata->max_val_per_block * col.data_size));
      }
    }

  TupleTableSlot* next(TupleTableSlot* slot) {
    auto& rfs = metadata->rfs;
    auto& relevant_blocks = metadata->relevant_blocks;
    auto* tuple_desc = slot->tts_tupleDescriptor;
    while(true) {
      if (cur_row_in_block >= num_rows_in_block) {
        if (next_block_idx >= relevant_blocks.size()) {
          return nullptr;
        }
        auto next_block = relevant_blocks[next_block_idx++];
        for(size_t attr_idx = 0; attr_idx < v_attrs_combined.size(); attr_idx++) {
          auto attrnum = v_attrs_combined[attr_idx];
          auto &col_data = metadata->column_data[attrnum];
          auto &fs = fds[attrnum];
          fs.seekg(col_data.block_offset_list[next_block]);
          fs.read(block_data[attrnum], col_data.row_count_list[next_block] * col_data.data_size);
          num_rows_in_block = col_data.row_count_list[next_block];
        }
        cur_row_in_block = 0;
      }

      bool found = true;
      for(size_t attr_idx = 0; attr_idx < v_attrs_used.size(); attr_idx++) {
        auto attrnum = v_attrs_used[attr_idx];
        auto &col_data = metadata->column_data[attrnum];

        Datum data;
        extract_next_datum(block_data[attrnum], col_data, cur_row_in_block, data);
        for(auto &rf: rfs[attrnum]) {

          Datum     const_val = rf.const_val->constvalue;
          int       collid = rf.const_val->constcollid;
          int       strategy = rf.strategy;
          FmgrInfo* finfo = &rf.finfo;
          bool      neg = rf.neg;

          int     cmpres;
          bool    satisfies;
          cmpres = FunctionCall2Coll(finfo, collid, const_val, data);
          switch(strategy) {
            case BTLessStrategyNumber:
              {
                satisfies = (cmpres > 0) ^ neg;
                break;
              }
            case BTLessEqualStrategyNumber:
              {
                satisfies = (cmpres >= 0) ^ neg;
                break;
              }
            case BTGreaterStrategyNumber:
              {
                satisfies = (cmpres < 0) ^ neg;
                break;
              }
            case BTGreaterEqualStrategyNumber:
              {
                satisfies = (cmpres <= 0) ^ neg;
                break;
              }
            case BTEqualStrategyNumber:
              {
                satisfies = (cmpres == 0) ^ neg;
                break;
              }
            default: Assert(false);
          }
          found = satisfies;
          if (not found) { break; }
        }
        if (not found) { break; }
      }
      if(not found) {
        cur_row_in_block++;
      } else {
        break;
      }
    }

    for(int attrnum = 0; attrnum < tuple_desc->natts; ++attrnum) {
      slot->tts_isnull[attrnum] = true;
    }

    for(size_t attr_idx = 0; attr_idx < v_attrs_returned.size(); attr_idx++) {
      auto attrnum = v_attrs_returned[attr_idx];
      auto &col_data = metadata->column_data[attrnum];
      extract_next_datum(block_data[attrnum], col_data, cur_row_in_block, slot->tts_values[attrnum]);
      slot->tts_isnull[attrnum] = false;
    }
    cur_row_in_block++;
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

void initialize(Oid foreigntableid, TupleDescData* tupleDesc, string &filename) {
  int metadata_size = 0;
  ifstream fs (filename, ifstream::binary);
  // Get metadata size
  fs.seekg(-4, fs.end);
  fs.read((char*)(&metadata_size), 4);

  // Get metadata as json
  fs.seekg(-1*(metadata_size + 4), fs.end);
  char* metadata_str = new char[metadata_size + 1]();
  fs.read(metadata_str, metadata_size);

  auto json = JsonType::parse(metadata_str);
  gMetadataMap.emplace(foreigntableid, db721_TableMetadata{json, tupleDesc, filename});
  delete metadata_str;
}

string get_foreign_table_info(Oid foreigntableid) {
  ForeignTable* table = GetForeignTable(foreigntableid);
  auto opt_list = table->options;
  ListCell* lc;
  foreach(lc, opt_list) {
    auto def = (DefElem*) lfirst(lc);
    if (string(def->defname) == "filename") {
      return string(defGetString(def));
    }
  }
  debug("Filename not found");
  assert(false);
}

int get_strategy(Oid vartype, Oid op_number) {
  auto op_class = GetDefaultOpClass(vartype, BTREE_AM_OID);
  if (not OidIsValid(op_class)) return 0;
  auto op_family = get_opclass_family(op_class);
  return get_op_opfamily_strategy(op_number, op_family);
}

// TODO: Can try combining row filters for the same AttrNumber
void extract_row_filters(RelOptInfo* baserel, vector<vector<RowFilter>> &rfs) {
  ListCell* lc;
  auto* all_clauses = baserel->baserestrictinfo;
  foreach(lc, all_clauses) {
    Expr *left, *right;
    Var* v;
    RelabelType* rt;
    Const *c;
    Oid opno;
    Expr *clause = (Expr*) lfirst(lc);

    if (IsA(clause, RestrictInfo)) {
      clause = ((RestrictInfo*)clause)->clause;
    }

    if (IsA(clause, OpExpr)) {
      auto *expr = (OpExpr*)clause;
      if (list_length(expr->args) != 2) continue;

      left = (Expr*) linitial(expr->args);
      right = (Expr*) lsecond(expr->args);

      if (IsA(right, Const))
      {
        if (IsA(left, RelabelType)) {
          rt = (RelabelType*) left;
          if (IsA(rt->arg, Var)) {
            v = (Var*)(rt->arg);
          } else {
            continue;
          }
        }
        else if (IsA(left, Var)) {
          v = (Var *) left;
        } else {
          continue;
        }
        c = (Const *) right;
        opno = expr->opno;
      }
      else if (IsA(left, Const))
      {
        if (IsA(right, RelabelType)) {
          rt = (RelabelType*) right;
          if (IsA(rt->arg, Var)) {
            v = (Var*)(rt->arg);
          } else {
            continue;
          }
        }
        else if (IsA(right, Var)) {
          v = (Var *) right;
        } else {
          continue;
        }
        c = (Const *) left;
        opno = get_commutator(expr->opno);
      } else {
        continue;
      }
      auto* tce_c = lookup_type_cache(c->consttype, TYPECACHE_BTREE_OPFAMILY);
      auto* tce_v = lookup_type_cache(v->vartype, TYPECACHE_BTREE_OPFAMILY);
      auto cmp_proc_oid = get_opfamily_proc(tce_v->btree_opf, tce_c->btree_opintype, tce_v->btree_opintype, BTORDER_PROC);

      int strategy = get_strategy(v->vartype, opno);
      if (strategy == 0) {
        opno = get_negator(opno);
        strategy = get_strategy(v->vartype, opno);
        if (strategy == 0) continue;
        rfs[v->varattno - 1].emplace_back(v, c, strategy, cmp_proc_oid, true);
        debug("Adding NEG filter " + to_string(strategy) + " for Varratno " + to_string(v->varattno - 1));
      } else {
        rfs[v->varattno - 1].emplace_back(v, c, strategy, cmp_proc_oid, false);
        debug("Adding filter " + to_string(strategy) + " for Varratno " + to_string(v->varattno - 1));
      }
    }
  }
}

void get_relevant_attrs(RelOptInfo* baserel) {
  db721_QueryPlan* query_plan = (db721_QueryPlan*) baserel->fdw_private;
  pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &query_plan->attrs_returned);
  ListCell *lc;
  foreach(lc, baserel->baserestrictinfo)
  {
    RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
    pull_varattnos((Node *) rinfo->clause, baserel->relid, &query_plan->attrs_used);
  }
}

// Only have one plan so costs don't really matter
pair<Cost, Cost> estimate_costs(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
  return make_pair(0.0, 0.0);
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
    Oid foreigntableid) {
  auto rte = root->simple_rte_array[baserel->relid];
  auto rel = table_open(rte->relid, AccessShareLock);
  auto tupleDesc = RelationGetDescr(rel);

  auto filename = get_foreign_table_info(foreigntableid);

  if (gMetadataMap.find(foreigntableid) == gMetadataMap.end()) {
    initialize(foreigntableid, tupleDesc, filename);
  }
  table_close(rel, AccessShareLock);

  auto& metadata = gMetadataMap[foreigntableid];
  // TODO: Move cleanup to the db721_EndForeignScan
  metadata.rfs.resize(metadata.column_data.size());
  extract_row_filters(baserel, metadata.rfs);
  metadata.extract_relevant_blocks();


  baserel->fdw_private = &metadata;
  baserel->reltarget->width = metadata.row_width;
  baserel->tuples = metadata.num_rows;

  // TODO: Use zone maps to find better estimates
  baserel->rows = metadata.num_rows;
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
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
  debug("Beginning scan! again");
  scan_clauses = extract_actual_clauses(scan_clauses, false);

  auto* query_plan = (db721_QueryPlan*) baserel->fdw_private;
  auto* ss = new db721_ScanState(query_plan->metadata);
  AttrNumber attr = -1;
  set<AttrNumber> all_attr;
  while ((attr = bms_next_member(query_plan->attrs_used, attr)) >= 0) {
    ss->v_attrs_used.push_back(attr - (1 - FirstLowInvalidHeapAttributeNumber));
    all_attr.insert(attr - (1 - FirstLowInvalidHeapAttributeNumber));
  }
  // Better ordering based on the attr types could be used
  sort(ss->v_attrs_used.begin(), ss->v_attrs_used.end());
  attr = -1;
  while ((attr = bms_next_member(query_plan->attrs_returned, attr)) >= 0) {
    ss->v_attrs_returned.push_back(attr - (1 - FirstLowInvalidHeapAttributeNumber));
    all_attr.insert(attr - (1 - FirstLowInvalidHeapAttributeNumber));
  }
  sort(ss->v_attrs_returned.begin(), ss->v_attrs_returned.end());
  ss->v_attrs_combined = vector(all_attr.begin(), all_attr.end());

  baserel->fdw_private = ss;

  // TODO: Free used memory
  // pfree(query_plan);

  return make_foreignscan(tlist, nullptr, baserel->relid, nullptr, (List*)(baserel->fdw_private), nullptr, nullptr, outer_plan);
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  ForeignScan* plan = (ForeignScan*) node->ss.ps.plan;
  node->fdw_state = plan->fdw_private;
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
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
  db721_ScanState* ss = (db721_ScanState*)node->fdw_state;
  ss->metadata->rfs.clear();
  ss->metadata->relevant_blocks.clear();
  for(size_t attr_idx = 0; attr_idx < ss->v_attrs_combined.size(); attr_idx++) {
    pfree(ss->block_data[attr_idx]);
  }
  delete ss;
}
