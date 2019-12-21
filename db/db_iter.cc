// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/filename.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter: public Iterator {//DHQ: 上面提到了 “multiple entries for the same userkey ”，Iter是能看到同一个 userkey的多个 k/v的
 public: //DHQ: DBIter 主要是个封装 Reverse，snapshot这些逻辑. 其他内部的Iter ，不管这些逻辑，只有简单的遍历
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction {
    kForward,
    kReverse
  };
  //DHQ: DBIter，封装了内部的iter_，是个 MergingIterator，MergingIterator不了解seqence，userkey，只有DBIter关注这个
  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_counter_(RandomPeriod()) {
  }
  virtual ~DBIter() {
    delete iter_;
  }
  virtual bool Valid() const { return valid_; }
  virtual Slice key() const {
    assert(valid_);//如果当前是kForward，saved_key_无效，需要从iter_取
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_; //只有当前为 kReverse时，saved_key_才保证有效
  }
  virtual Slice value() const {
    assert(valid_);//saved_value，与上面saved_key_ 使用类似。仅仅在 kReverse 才有效
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);//DHQ: free string's space.
    } else {
      saved_value_.clear();
    }
  }

  // Pick next gap with average value of config::kReadBytesPeriod.
  ssize_t RandomPeriod() {
    return rnd_.Uniform(2*config::kReadBytesPeriod);
  }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  Iterator* const iter_; //DHQ: 这个是个 internal iter
  SequenceNumber const sequence_;

  Status status_;
  std::string saved_key_;     // == current key when direction_==kReverse  仅在 kReverse 时有效
  std::string saved_value_;   // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;

  Random rnd_;
  ssize_t bytes_counter_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();
  ssize_t n = k.size() + iter_->value().size();
  bytes_counter_ -= n;
  while (bytes_counter_ < 0) {
    bytes_counter_ += RandomPeriod();
    db_->RecordReadSample(k); //这个是根据读的一些统计量，来触发compaction的，我们不用这个
  }
  if (!ParseInternalKey(k, ikey)) {//DHQ: InternalKey，从 slice 转结构体
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {//应该反方向遍历过头了
      iter_->SeekToFirst();//回到first
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {//直接就是无效的
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {//DHQ: 上次也是Forward，则直接从上次的key(作为 saved_key_ )，开始找，必须跳过上次的user_key，不重复返回
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_); //临时save的，让 FindNextUserEntry 跳过当前user key
  }

  FindNextUserEntry(true, &saved_key_);//true，表明这个saved_key就要被skip掉。 saved_key_作为skip使用，因为需要跳过相同的user_key
}
//DHQ: 避免两次获得相同的user_key，所以 上面调用时，参数skipping = true
void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {//如果有k1, k2, k3，当前时k1, k2 被del了(<sequence_的最大的，是Del)，那么应返回k3。k2的多个sequnce，都需要被skip掉。
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {//DHQ:如果Del的seqno > sequence_，那么不应该造成skip。快照含义如此
      switch (ikey.type) {
        case kTypeDeletion://DHQ: 遇到一个 delete，在kForward模式下，seqno 大的先遇到
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);//本循环后续，会遇到相同user_key的，判断skip
          skipping = true;
          break;//switch
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {//DHQ: skip再不同的情况下（上一个操作时prev还是next），含义不同，需要被 skip掉
            // Entry hidden
          } else {//DHQ: 当前不在skip状态，或者换了 user key
            valid_ = true;
            saved_key_.clear();//这个也clear了，不是总有值，虽然是valid的, TODO: 下次key()，返回的是什么？
            return;//DHQ:找到了，直接return
          }
          break; //switch
      }
    }
    iter_->Next(); //DHQ: 实际上是上面 if 的 else
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false; //DHQ: 走到这里，肯定时invalid的
}

void DBIter::Prev() {
  assert(valid_);
  //TODO： 为什么这里要求assert(iter_->Valid())，而 Next不是？
  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear(); //DHQ: 分别 clear saved_key 和 saved_value
        ClearSavedValue();//因为可能较大，所以可能释放 saved_value_的内存
        return; //invalid， 直接 return 了，没有改变direction_
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()),
                                    saved_key_) < 0) {//DHQ: 直到user key改变了（小于saved_key_），才能break
        break;
      }
    }
    direction_ = kReverse;
  }//else，表明上次不是Forward，那么saved_key_应该也有值，因为valid_
  //上面循环到此时，已经找到了一个 user key < saved_key_的 key，并且是valid的，此时iter应该位于一个小于saved_key_的最小seqno的位置。否则，上面就return了。
  FindPrevUserEntry();
}
//DHQ:参见"since we sort sequence numbers in decreasing order..."， prev是不同的

//与next的主要差别是：prev 最先遇到的是 seqno 小的key，而不是大的。所以，只有iter_遇到更小的user key，才能结束循环，但是结束时，不能使用iter_的key了。只有搞个saved_key_
//*********正常的prev操作，iter_和saved_key_不是同一个位置(iter_靠左)，重要*************
//*** 这是因为，prev操作，只有往左遇到user key变化了，才能判断是否结束循环。而此时iter_已经指向左边key，saved_key_则是要返回的key
//当然，如果左边已经没有key了，那么iter_是invalid. iter_ 变为Invalid只是结束向左循环探测的条件，不代表DBIter是invalid的，此时saved_key_是有效的，可以正常返回key()

//这个函数，被Prev和SeekForPrev调用。但是进入此函数时，情况不同。
//假设有key 'A'和'B'
//Case 1) 如果之前执行了执行Seek('B')，现在执行Prev()，则此时 iter_已经指向了'A'，而不是'B',但是saved_key_是'B'
//Case 2) 对于SeekForPrev('B')操作，进入此函数时，iter_指向的就是'B'
//Case 3) 对于SeekToLast()操作，进入此函数时，iter_指向'B'右边
//虽然名字为FindPrev，但是对于Case 2，实际上找到的是'B'(如果key的sequence满足)

//函数返回时，如果saved_key_是x，那么iter_又指向了x左边的key或者Invalid了，而不是x
void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;  //注意这个value_type的赋值，是表示上一个key的type，不是对应ikey的type。实在Compare()之后，才赋值的
  if (iter_->Valid()) {//因为要保证发生了user key变化(不是之前那个)，所以比较复杂
    do {
      ParsedInternalKey ikey;//下面的Compare，如果一个user key连续出现两次，那么不会break，因为不满足 Compare(ikey.user_key, saved_key_) < 0
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {//只考虑 <= sequence_的, 大的过滤掉
        if ((value_type != kTypeDeletion) && //上一个key，不是 kTypeDeletion(即saved_key_有值)，并且本次的user_key比上次saved_key_小(即换了key)
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {//DHQ: 找到了一个小于save_key的且不是delete的，成功
          // We encountered a non-deleted value in entries for previous keys,
          break; //这个函数是prev()调用的，在break后，key()函数返回的是saved_key_，不是ikey.user_key。
        }
        value_type = ikey.type;//注意！！！！！这里才修改 value_type，应该改名为 last_value_type!
        if (value_type == kTypeDeletion) {
          saved_key_.clear(); //clear过后，下次循环到上面语句，user_comparator_->Compare 总失败？
          ClearSavedValue();  //TODO: 如果进入函数时，当前key是C，并且没有seq更大的了，(B, 103, v3), (B, 102, V2), (B, 100, Del)，先找到 (B, 100, Del)
        if ((value_type != kTypeDeletion) && //上一个key，不是 kTypeDeletion(即saved_key_有值)，并且本次的user_key比上次saved_key_小
        } else { //这里判断的是ikey.type，value_type 已经变了
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_); //DHQ: 跟临时变量交换，让临时变量释放空间
          }//保留这个可以作为saved_key_，然后接着往前找，直到：user_key变了，说明当前user_key的最大seqno已经被找到，然后key()返回的是saved_key_
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {//循环执行了0次的情况，才会走这个分支
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;//实际上是遇到prev的prev或者iter无效后后，才停止的。这时候不需要再设置saved_key_，因为已经在之前设置完了
  }
}
//DHQ: 因为要找的是 <= 给定seq的有效的key(非DEL)，所以FindNextUserEntry去除无效的
void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(
      &saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {//DHQ: 刚刚做了seek，不需要skip saved_key_，只是为了找到有效的而已
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

}  // anonymous namespace

Iterator* NewDBIterator(
    DBImpl* db,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    SequenceNumber sequence,
    uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
