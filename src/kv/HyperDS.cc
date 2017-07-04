// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * In-memory crash non-safe keyvalue db
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#include "include/compat.h"
#include <set>
#include <map>
#include <string>
#include <memory>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "common/perf_counters.h"
#include "common/debug.h"
#include "include/str_list.h"
#include "include/str_map.h"
#include "KeyValueDB.h"
#include "HyperDS.h"

#include "include/assert.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/compat.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_memdb
#undef dout_prefix
#define dout_prefix *_dout << "memdb: "
#define dtrace dout(30)
#define dwarn dout(0)
#define dinfo dout(0)

static void split_key(const string& raw_key, string *prefix, string *key)
{
  size_t pos = raw_key.find(KEY_DELIM, 0);
  assert(pos != std::string::npos);
  *prefix = raw_key.substr(0, pos);
  *key = raw_key.substr(pos + 1, raw_key.length());
}

static string make_key(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(KEY_DELIM);
  out.append(value);
  return out;
}

void HyperDS::_encode(mdb_iter_t iter, bufferlist &bl)
{
  ::encode(iter->first, bl);
  ::encode(iter->second, bl);
}

std::string HyperDS::_get_data_fn()
{
  string fn = m_db_path + "/" + "HyperDS.db";
  return fn;
}

void HyperDS::_save()
{
  std::lock_guard<std::mutex> l(m_lock);
  dout(10) << __func__ << " Saving HyperDS to file: "<< _get_data_fn().c_str() << dendl;
  int mode = 0644;
  int fd = TEMP_FAILURE_RETRY(::open(_get_data_fn().c_str(),
                                     O_WRONLY|O_CREAT|O_TRUNC, mode));
  if (fd < 0) {
    int err = errno;
    cerr << "write_file(" << _get_data_fn().c_str() << "): failed to open file: "
         << cpp_strerror(err) << std::endl;
    return;
  }
  bufferlist bl;
  mdb_iter_t iter = m_map.begin();
  while (iter != m_map.end()) {
    dout(10) << __func__ << " Key:"<< iter->first << dendl;
    _encode(iter, bl);
    ++iter;
  }
  bl.write_fd(fd);

  VOID_TEMP_FAILURE_RETRY(::close(fd));
}

int HyperDS::_load()
{
  std::lock_guard<std::mutex> l(m_lock);
  dout(10) << __func__ << " Reading HyperDS from file: "<< _get_data_fn().c_str() << dendl;
  /*
   * Open file and read it in single shot.
   */
  int fd = TEMP_FAILURE_RETRY(::open(_get_data_fn().c_str(), O_RDONLY));
  if (fd < 0) {
    int err = errno;
    cerr << "can't open " << _get_data_fn().c_str() << ": "
         << cpp_strerror(err) << std::endl;
    return -err;
  }

  struct stat st;
  memset(&st, 0, sizeof(st));
  if (::fstat(fd, &st) < 0) {
    int err = errno;
    cerr << "can't stat file " << _get_data_fn().c_str() << ": "
         << cpp_strerror(err) << std::endl;
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return -err;
  }

  ssize_t file_size = st.st_size;
  ssize_t bytes_done = 0;
  while (bytes_done < file_size) {
    string key;
    bufferptr datap;

    bytes_done += ::decode_file(fd, key);
    bytes_done += ::decode_file(fd, datap);

    dout(10) << __func__ << " Key:"<< key << dendl;
    m_map[key] = datap;
    m_total_bytes += datap.length();
  }
  VOID_TEMP_FAILURE_RETRY(::close(fd));

  //#### HYPERDS #####
  kvdb::Iterator* h_it = h_db->NewIterator();
  for(h_it->SeekToFirst(); h_it->Valid(); h_it->Next()) {
      h_set.insert(h_it->Key());
  }
  delete h_it;
  dout (1) << __func__ << " HyperDS iterator get key num:" << h_set.size() << " mdb key num: " << m_map.size() << dendl;
  mdb_iter_t iter = m_map.begin();
  bool all_in = true;
  while(iter!=m_map.end()) {
    if ( h_set.find(iter->first) == h_set.end()) {
        dout (1) << __func__ << " HyperDS iterator Not Find key:" << iter->first <<dendl;
        all_in =false;
    } else {
      string data;
      h_db->Get(iter->first.c_str(), iter->first.length(), data);
      int is_same = memcmp(data.c_str(), iter->second.c_str(), data.length()); 
      if ( is_same ) {
        dout(1) << __func__ << " HyperDS INCONSISTENT!!!!" << dendl;
        assert(is_same == 0);
      } 
      dout (1) << __func__ << " HyperDS iterator Find key:" << iter->first <<dendl;
    }
    ++iter;
  }
  if (all_in) {
    dout (1) << __func__ << " HyperDS iterator all in" <<dendl;
  } else {
    dout (1) << __func__ << " HyperDS NOTTTTTTTT ALL IN" << dendl;
  }
  //#### HYPERDS #####

  return 0;
}

int HyperDS::_init(bool create)
{
  //#### HYPERDS #####
  if (create) {
    dout(1) << " HyperDS Create db..." << dendl;
    if (!kvdb::DB::CreateDB(HYPERDS_PATH)) {
      dout(1) << " HyperDS Create db Failed" << dendl;
      int err = errno;
      cerr << " HyperDS Create db Failed" << std::endl;
      return err;
    }
    dout(1) << " HyperDS Create db Success!" << dendl;
  } 

  dout(1) << " HyperDS Open db..." << dendl;
  if (!kvdb::DB::OpenDB(HYPERDS_PATH, &h_db)) {
    int err = errno;
    cerr << " HyperDS Open db Failed" << std::endl;
    return err;
  }
  dout(1) << " HyperDS Open db Success!" << dendl;
  //#### HYPERDS #####

  int r;
  dout(1) << __func__ << dendl;
  if (create) {
    r = ::mkdir(m_db_path.c_str(), 0700);
    if (r < 0) {
      r = -errno;
      if (r != -EEXIST) {
        derr << __func__ << " mkdir failed: " << cpp_strerror(r) << dendl;
        return r;
      }
      return 0; // ignore EEXIST
    }
  } else {
    r = _load();
  }

  return r;
}

int HyperDS::set_merge_operator(
  const string& prefix,
  std::shared_ptr<KeyValueDB::MergeOperator> mop)
{
  merge_ops.push_back(std::make_pair(prefix, mop));
  return 0;
}

int HyperDS::do_open(ostream &out, bool create)
{
  m_total_bytes = 0;
  m_allocated_bytes = 1;

  return _init(create);
}

HyperDS::~HyperDS()
{
  close();
  dout(10) << __func__ << " Destroying HyperDS instance: "<< dendl;
}

void HyperDS::close()
{
  /*
   * Save whatever in memory btree.
   */
  _save();
  //#### HYPERDS #####
  if (h_db) {
    delete h_db;
    dout(1) << __func__ << " HyperDS delete h_db" << dendl;
    h_db = nullptr;
  }
  //#### HYPERDS #####
}

int HyperDS::submit_transaction(KeyValueDB::Transaction t)
{
  MDBTransactionImpl* mt =  static_cast<MDBTransactionImpl*>(t.get());

  dtrace << __func__ << " " << mt->get_ops().size() << dendl;
  for(auto& op : mt->get_ops()) {
    if(op.first == MDBTransactionImpl::WRITE) {
      ms_op_t set_op = op.second;
      _setkey(set_op);
    } else if (op.first == MDBTransactionImpl::MERGE) {
      ms_op_t merge_op = op.second;
      _merge(merge_op);
    } else {
      ms_op_t rm_op = op.second;
      assert(op.first == MDBTransactionImpl::DELETE);
      _rmkey(rm_op);
    }
  }

  //#### HYPERDS #####
  //kvdb::Status h_s = h_db->InsertBatch(&bat);
  //if(!h_s.ok()) {
  //  dout(1) << __func__ << "HyperDS Insert Write BatchERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << dendl;
  //}

  //bat.clear();
  //dout(1) << __func__ << " HyperDS Insert Write Batch" << dendl;
  //#### HYPERDS #####

  return 0;
}

int HyperDS::submit_transaction_sync(KeyValueDB::Transaction tsync)
{
  dtrace << __func__ << " " << dendl;
  submit_transaction(tsync);
  return 0;
}

int HyperDS::transaction_rollback(KeyValueDB::Transaction t)
{
  MDBTransactionImpl* mt =  static_cast<MDBTransactionImpl*>(t.get());
  mt->clear();
  return 0;
}

void HyperDS::MDBTransactionImpl::set(
  const string &prefix, const string &k, const bufferlist &to_set_bl)
{
  dtrace << __func__ << " " << prefix << " " << k << dendl;
  ops.push_back(make_pair(WRITE, std::make_pair(std::make_pair(prefix, k),
                  to_set_bl)));
}

void HyperDS::MDBTransactionImpl::rmkey(const string &prefix,
    const string &k)
{
  dtrace << __func__ << " " << prefix << " " << k << dendl;
  ops.push_back(make_pair(DELETE,
                          std::make_pair(std::make_pair(prefix, k),
                          bufferlist())));
}

void HyperDS::MDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = m_db->get_iterator(prefix);
  for (it->seek_to_first(); it->valid(); it->next()) {
    rmkey(prefix, it->key());
  }
}

void HyperDS::MDBTransactionImpl::rm_range_keys(const string &prefix, const string &start, const string &end)
{
  KeyValueDB::Iterator it = m_db->get_iterator(prefix);
  it->lower_bound(start);
  while (it->valid()) {
    if (it->key() >= end) {
      break;
    }
    rmkey(prefix, it->key());
    it->next();
  }
}

void HyperDS::MDBTransactionImpl::merge(
  const std::string &prefix, const std::string &key, const bufferlist  &value)
{

  dtrace << __func__ << " " << prefix << " " << key << dendl;
  ops.push_back(make_pair(MERGE, make_pair(std::make_pair(prefix, key), value)));
  return;
}

int HyperDS::_setkey(ms_op_t &op)
{
  std::lock_guard<std::mutex> l(m_lock);
  std::string key = make_key(op.first.first, op.first.second);
  bufferlist bl = op.second;

  m_total_bytes += bl.length();

  bufferlist bl_old;
  dout(1) << __func__ << " HyperDS _setkey will call _get " << dendl;
  if (_get(op.first.first, op.first.second, &bl_old)) {
    /*
     * delete and free existing key.
     */
    assert(m_total_bytes >= bl_old.length());
    m_total_bytes -= bl_old.length();
    m_map.erase(key);
  }

  m_map[key] = bufferptr((char *) bl.c_str(), bl.length());
  iterator_seq_no++;

  //#### HYPERDS #####
  bat.put(key.c_str(), key.length(), bl.c_str(), bl.length() );
  //h_db->InsertBatch(&bat);
  kvdb::Status h_s = h_db->InsertBatch(&bat);
  if(!h_s.ok()) {
    dout(1) << __func__ << "HyperDS Insert Write BatchERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << dendl;
  }
  bat.clear();
  std::string get_aaa;
  h_db->Get(key.c_str(), key.length(), get_aaa);

  int is_same = memcmp(get_aaa.c_str(), bl.c_str(), get_aaa.length());
  //assert(is_same == 0);
  if ( is_same ) {
    dout(1) << __func__ << " HyperDS WRONG!!!!!!!!" << dendl;
  }

  std::string pt_value(bl.c_str(), bl.length());
  dout(1) << __func__ << " HyperDS hyperds set key: " << key << dendl;
  //dout(1) << __func__ << " HyperDS hyperds set key: " << key << ", Length: " << key.length() << " hyperds_length = " << get_aaa.length()  << "memdb_length = " << bl.length() <<  "value: " << pt_value <<  "hyper_value: " << get_aaa << dendl;
  //dout(1) << __func__ << " HyperDS hyperds set key: " << key << ", Length: " << bl.length() << ", value: "<< pt_value <<  dendl;
  //#### HYPERDS #####
  return 0;
}

int HyperDS::_rmkey(ms_op_t &op)
{
  std::lock_guard<std::mutex> l(m_lock);
  std::string key = make_key(op.first.first, op.first.second);

  bufferlist bl_old;
  dout(1) << __func__ << " HyperDS _rmkey will call _get " << dendl;
  if (_get(op.first.first, op.first.second, &bl_old)) {
    assert(m_total_bytes >= bl_old.length());
    m_total_bytes -= bl_old.length();
  }
  iterator_seq_no++;
  /*
   * Erase will call the destructor for bufferptr.
   */
  //#### HYPERDS #####
  bat.del(key.c_str(), key.length());
  //h_db->InsertBatch(&bat);
  kvdb::Status h_s = h_db->InsertBatch(&bat);
  if(!h_s.ok()) {
    dout(1) << __func__ << "HyperDS Insert Write BatchERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << dendl;
  }
  bat.clear();
  dout(1) << __func__ << " HyperDS hyperds del key: " << key << dendl;
  //#### HYPERDS #####
  return m_map.erase(key);
}

std::shared_ptr<KeyValueDB::MergeOperator> HyperDS::_find_merge_op(std::string prefix)
{
  for (const auto& i : merge_ops) {
    if (i.first == prefix) {
      return i.second;
    }
  }

  dtrace << __func__ << " No merge op for " << prefix << dendl;
  return NULL;
}


int HyperDS::_merge(ms_op_t &op)
{
  std::lock_guard<std::mutex> l(m_lock);
  std::string prefix = op.first.first;
  std::string key = make_key(op.first.first, op.first.second);
  bufferlist bl = op.second;
  int64_t bytes_adjusted = bl.length();

  /*
   *  find the operator for this prefix
   */
  std::shared_ptr<MergeOperator> mop = _find_merge_op(prefix);
  assert(mop);

  /*
   * call the merge operator with value and non value
   */
  bufferlist bl_old;
  dout(1) << __func__ << " HyperDS _mergekey will call _get " << dendl;
  if (_get(op.first.first, op.first.second, &bl_old) == false) {
    std::string new_val;
    /*
     * Merge non existent.
     */
    mop->merge_nonexistent(bl.c_str(), bl.length(), &new_val);
    //m_map[key] = bufferptr(new_val.c_str(), new_val.length());

    //#### HYPERDS #####
    bat.put(key.c_str(), key.length(), new_val.c_str(), new_val.length() );
    //h_db->InsertBatch(&bat);
    kvdb::Status h_s = h_db->InsertBatch(&bat);
    if(!h_s.ok()) {
      dout(1) << __func__ << "HyperDS Insert Write BatchERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << dendl;
    }
    bat.clear();
    std::string get_aaa;
    h_db->Get(key.c_str(), key.length(), get_aaa);

    int is_same = memcmp(get_aaa.c_str(), new_val.c_str(), get_aaa.length());
    if ( is_same ) {
      dout(1) << __func__ << " HyperDS WRONG!!!!!!!!" << dendl;
      assert(is_same == 0);
    }

    dout(1) << __func__ << " HyperDS hyperds Merge (non existent) key: " << key << dendl;
    //dout(1) << __func__ << " HyperDS hyperds Merge (non existent) key: " << key << ", Length: " << get_aaa.length()  << "memdb_length = " << bl.length() <<  "value: " << new_val <<  "hyper_value: " << get_aaa << dendl;
    //dout(1) << __func__ << " HyperDS hyperds Merge (non existent) key: " << key << ", Length: " << key.length()  << "memdb_length = " << bl.length() <<  "value: " << new_val << dendl;
    //#### HYPERDS #####
    m_map[key] = bufferptr(new_val.c_str(), new_val.length());
  } else {
    /*
     * Merge existing.
     */
    std::string new_val;
    mop->merge(bl_old.c_str(), bl_old.length(), bl.c_str(), bl.length(), &new_val);
    //m_map[key] = bufferptr(new_val.c_str(), new_val.length());
    //bytes_adjusted -= bl_old.length();
    //bl_old.clear();

    //#### HYPERDS #####
    bat.put(key.c_str(), key.length(), new_val.c_str(), new_val.length() );
    //h_db->InsertBatch(&bat);
    kvdb::Status h_s = h_db->InsertBatch(&bat);
    if(!h_s.ok()) {
      dout(1) << __func__ << "HyperDS Insert Write BatchERROR!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << dendl;
    }
    bat.clear();
    std::string get_aaa;
    h_db->Get(key.c_str(), key.length(), get_aaa);

    int is_same = memcmp(get_aaa.c_str(), new_val.c_str(), get_aaa.length());
    if ( is_same ) {
      dout(1) << __func__ << " HyperDS WRONG!!!!!!!!" << dendl;
      assert(is_same == 0);
    }

    dout(1) << __func__ << " HyperDS hyperds Merge (existing) key: " << key << dendl;
    //dout(1) << __func__ << " HyperDS hyperds Merge (existing) key: " << key << ", Length: " << get_aaa.length() << "memdb_length = " << bl_old.length()+bl.length()<< "value: " << new_val << "hyper_value: " << get_aaa << dendl;
    //dout(1) << __func__ << " HyperDS hyperds Merge (existing) key: " << key << ", Length: " << key.length()  << "memdb_length = " << bl.length() <<  "value: " << new_val << dendl;
    //#### HYPERDS #####
    m_map[key] = bufferptr(new_val.c_str(), new_val.length());
    bytes_adjusted -= bl_old.length();
    bl_old.clear();
  }

  assert((int64_t)m_total_bytes + bytes_adjusted >= 0);
  m_total_bytes += bytes_adjusted;
  iterator_seq_no++;
  return 0;
}

/*
 * Caller take btree lock.
 */
bool HyperDS::_get(const string &prefix, const string &k, bufferlist *out)
{
  string key = make_key(prefix, k);

  mdb_iter_t iter = m_map.find(key);
  if (iter == m_map.end()) {
    return false;
  }


  out->push_back((m_map[key].clone()));

  //#### HYPERDS #####
  string data;
  h_db->Get(key.c_str(), key.length(), data);
  std::string pt_str(out->c_str(), out->length());
  int is_same = memcmp(data.c_str(), out->c_str(), data.length());
  if ( is_same ) {
  dout(1) << __func__ << " HyperDS hyperds Inconsistent get key: " << key <<dendl;
  assert(is_same == 0);
  //dout(1) << __func__ << " HyperDS hyperds get key: " << key << ", value: " << data << "memdb_value: "<< pt_str <<", The data Inconsistent !!!!" << ",hyperds length is: " << data.length()  << "memdb_length : " << pt_str.length() << dendl;
  } else {
  dout(1) << __func__ << " HyperDS hyperds get key: " << key <<dendl;
  //dout(1) << __func__ << " HyperDS hyperds get key: " << key << ", value: " << data << "memdb_value: "<< pt_str <<", The data Same !!! " << ", hyperds length is: " << data.length()  << "memdb_length : " << pt_str.length() << dendl;
  }
  //#### HYPERDS #####
  return true;
}

bool HyperDS::_get_locked(const string &prefix, const string &k, bufferlist *out)
{
  std::lock_guard<std::mutex> l(m_lock);
  return _get(prefix, k, out);
}


int HyperDS::get(const string &prefix, const std::string& key,
                 bufferlist *out)
{
  if (_get_locked(prefix, key, out)) {
    return 0;
  }
  return -ENOENT;
}

int HyperDS::get(const string &prefix, const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  for (const auto& i : keys) {
    bufferlist bl;
    if (_get_locked(prefix, i, &bl))
      out->insert(make_pair(i, bl));
  }

  return 0;
}

void HyperDS::MDBWholeSpaceIteratorImpl::fill_current()
{
  bufferlist bl;
  bl.append(m_iter->second.clone());
  m_key_value = std::make_pair(m_iter->first, bl);
}

bool HyperDS::MDBWholeSpaceIteratorImpl::valid()
{
  if (m_key_value.first.empty()) {
    return false;
  }
  return true;
}

bool HyperDS::MDBWholeSpaceIteratorImpl::iterator_validate() {

  if (this_seq_no != *global_seq_no) {
    auto key = m_key_value.first;
    assert(!key.empty());

    bool restart_iter = false;
    if (!m_using_btree) {
      /*
       * Map is modified and marker key does not exists, 
       * restart the iterator from next key.
       */
      if (m_map_p->find(key) == m_map_p->end()) {
        restart_iter = true;
      }
    } else {
      restart_iter = true;
    }

    if (restart_iter) {
      m_iter = m_map_p->lower_bound(key);
      if (m_iter == m_map_p->end()) {
        return false;
      }
    }

    /*
     * This iter is valid now.
     */
    this_seq_no = *global_seq_no;
  }

  return true;
}

void
HyperDS::MDBWholeSpaceIteratorImpl::free_last()
{
  m_key_value.first.clear();
  m_key_value.second.clear();
}

string HyperDS::MDBWholeSpaceIteratorImpl::key()
{
  dtrace << __func__ << " " << m_key_value.first << dendl;
  string prefix, key;
  split_key(m_key_value.first, &prefix, &key);
  return key;
}

pair<string,string> HyperDS::MDBWholeSpaceIteratorImpl::raw_key()
{
  string prefix, key;
  split_key(m_key_value.first, &prefix, &key);
  return make_pair(prefix, key);
}

bool HyperDS::MDBWholeSpaceIteratorImpl::raw_key_is_prefixed(
    const string &prefix)
{
  string p, k;
  split_key(m_key_value.first, &p, &k);
  return (p == prefix);
}

bufferlist HyperDS::MDBWholeSpaceIteratorImpl::value()
{
  dtrace << __func__ << " " << m_key_value << dendl;
  return m_key_value.second;
}

int HyperDS::MDBWholeSpaceIteratorImpl::next()
{
  std::lock_guard<std::mutex> l(*m_map_lock_p);
  if (!iterator_validate()) {
    free_last();
    return -1;
  }
  free_last();
  ++m_iter;
  if (m_iter != m_map_p->end()) {
    fill_current();
    return 0;
  } else {
    return -1;
  }
}

int HyperDS::MDBWholeSpaceIteratorImpl:: prev()
{
  std::lock_guard<std::mutex> l(*m_map_lock_p);
  if (!iterator_validate()) {
    free_last();
    return -1;
  }
  free_last();
  if (m_iter != m_map_p->begin()) {
    --m_iter;
    fill_current();
    return 0;
  } else {
    return -1;
  }
}

/*
 * First key >= to given key, if key is null then first key in btree.
 */
int HyperDS::MDBWholeSpaceIteratorImpl::seek_to_first(const std::string &k)
{
  std::lock_guard<std::mutex> l(*m_map_lock_p);
  free_last();
  if (k.empty()) {
    m_iter = m_map_p->begin();
  } else {
    m_iter = m_map_p->lower_bound(k);
  }
  
  if (m_iter == m_map_p->end()) {
    return -1;
  }
  fill_current();
  return 0;
}

int HyperDS::MDBWholeSpaceIteratorImpl::seek_to_last(const std::string &k)
{
  std::lock_guard<std::mutex> l(*m_map_lock_p);
  free_last();
  if (k.empty()) {
    m_iter = m_map_p->end();
    --m_iter;
  } else {
    m_iter = m_map_p->lower_bound(k);
  }

  if (m_iter == m_map_p->end()) {
    return -1;
  }
  fill_current();
  return 0;
}

HyperDS::MDBWholeSpaceIteratorImpl::~MDBWholeSpaceIteratorImpl()
{
  free_last();
}

int HyperDS::MDBWholeSpaceIteratorImpl::upper_bound(const std::string &prefix,
    const std::string &after) {

  std::lock_guard<std::mutex> l(*m_map_lock_p);

  dtrace << "upper_bound " << prefix.c_str() << after.c_str() << dendl;
  string k = make_key(prefix, after);
  m_iter = m_map_p->upper_bound(k);
  if (m_iter != m_map_p->end()) {
    fill_current();
    return 0;
  }
  return -1;
}

int HyperDS::MDBWholeSpaceIteratorImpl::lower_bound(const std::string &prefix,
    const std::string &to) {
  std::lock_guard<std::mutex> l(*m_map_lock_p);
  dtrace << "lower_bound " << prefix.c_str() << to.c_str() << dendl;
  string k = make_key(prefix, to);
  m_iter = m_map_p->lower_bound(k);
  if (m_iter != m_map_p->end()) {
    fill_current();
    return 0;
  }
  return -1;
}

