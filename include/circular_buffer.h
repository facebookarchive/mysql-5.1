#ifndef _circular_buffer_h
#define _circular_buffer_h

#include "mysql_priv.h"
/**
 A circular_buffer maintains at most 'alloc_size' elements. When the rotating
 array is full, an insertion by calling push_back() will overwrite the
 circular buffer's current head, and will move the head to the old head's next.

 The circular buffer is using the 'always keep one slot open' approach.
 If both tail and head refer to the same slot, the buffer is empty. If the
 tail pointer refers to the slot which is right before the one referred to by
 the head pointer, the buffer is full.
 */
template <class T>
class circular_buffer
{
public:

  /**
   Constructor.

   @param alloc_size Number of elements in this array
   */
  circular_buffer(uint my_alloc_size)
    :size_(my_alloc_size + 1), head_(0),
     tail_(0)
  {
    array_ = new T[my_alloc_size+1];
  }

  ~circular_buffer()
  {
    delete[] array_;
  }

  /**
   Append a new element at the end of this array

   @param elem the element to be inserted
   @return the position this new value is inserted
   */
  int push_back(T elem)
  {
    array_[tail_] = elem;
    tail_ = (tail_ + 1) % size_;
    if(tail_ == head_)
      head_ = (head_ + 1) % size_;
    return tail_;
  }

  /**
   Remove the head. If not empty after the removal, a new head is
   at the old head's next position.

   @return return the old position of the head, -1 if empty
   */
  int remove_front()
  {
    if(empty())
      return -1;
    int old_head_pos = head_;
    head_ = (head_ + 1) % size_;
    return old_head_pos;
  }

  /**
  Scan this array and find the largest element's position.

   @return the max element's position in this array
   */
  int find_max()
  {
    if (empty())
      return -1;
    int current_pos = head_;
    int current_max_pos = head_;
    while (current_pos != tail_)
    {
      if (array_[current_max_pos] < array_[current_pos])
        current_max_pos = current_pos;
      current_pos = (current_pos + 1) % size_;
    }
    return current_max_pos;
  }

  /**
   Get the head

   @return NULL if no element, otherwise the head.
   */
  const T& front()
  {
    return array_[head_];
  }

  /**
   Get the tail

   @return NULL if no element, otherwise the tail.
   */
  const T& back()
  {
    return array_[tail_];
  }

  /**
   Get the element at the given position. Note that this function
   does not do any check on the position argument. So its the caller
   responsibility to make sure the give position is valid.

   @return the element at the given position
   */
  const T& get(int pos)
  {
    return array_[pos];
  }


  bool empty()
  {
    return (tail_ == head_);
  }

private:
  T* array_;
  int size_;
  /**
   head points to the position of the current buffer's head
   */
  int head_;
  /*
   tail points to the position where the next insert will happen
   */
  int tail_;

  circular_buffer(const circular_buffer& b);
  const circular_buffer& operator=(const circular_buffer& b);
};


/**
 A max sliding window. A max sliding window is defined by 3 parameters:
 1. the window interval. It defines how big is the window.
 2. max number of elements maintained in the window.
 3. window update rate. how often should the window be updated. It will only
   maintain the max value of all pending insertions if the window update rate
   is not reached.

 All elements in the window are associated with a window interval value at
 the time of insertion. When an insertion happens, the max sliding window
 will first check if the insert can really happen or not by checking if the
 'window update rate' is reached or not. If the update rate is not reached,
 it will just update the max_value since last insertion, and return false.
 If the update rate is reached, it will insert the max value since last
 insertion, and use the current arguments passed in start a new update
 interval, and return true to caller.


 */
template <class T, class NumericValue>
class max_sliding_window
{
public:
  /**
   Constructor

   @param capacity the maximum number of elements maintained
   @param interval the window interval
   */
  max_sliding_window(int capacity, NumericValue interval,
      NumericValue update_rate_by_interval)
    : window_interval_(interval),
      update_rate_ (update_rate_by_interval),
      max_val_since_last_update_ (0),
      last_interval_start_ (0),
      value_array_(new circular_buffer<T> (capacity)),
      interval_array_(new circular_buffer<NumericValue> (capacity))
  {
  }

  ~max_sliding_window()
  {
    delete value_array_;
    delete interval_array_;
  }

  /**
   Insert a pair of (element_value, window_value). Do a real insertion
   of the max sliding window only if the elapsed intervals since last
   update is >= 'update_rate'. If the update rate is not reached, the
   new_value is used to update the max_val_since_last_update, and return
   false. If the update rate is reached,  it will insert the max_value since
   last insertion and also update the window by removing all old values that
   fall out of the defined window interval.
   */
  bool push_back(T new_value, NumericValue current_interval_val)
  {

    if (max_val_since_last_update_ <= 0)
    {
      last_interval_start_ = current_interval_val;
      max_val_since_last_update_ = new_value;
      return false;
    }

    //do we need to update the window?
    NumericValue interval_diff = (long) (current_interval_val
        - last_interval_start_);

    //only update window if 'interval_diff >= update_rate'
    if (interval_diff < update_rate_)
    {
      if (new_value > max_val_since_last_update_)
      {
        //haven't reached the update rate, update the max value only
        max_val_since_last_update_ = new_value;
      }
      return false;
    }

    interval_array_->push_back(last_interval_start_);
    value_array_->push_back(max_val_since_last_update_);
    update_window(last_interval_start_);

    last_interval_start_ = current_interval_val;
    max_val_since_last_update_ = new_value;
    return true;
  }

  /**
   Returning the max element in the window that ends
   with current_interval_value. It will first use the
   'current_interval_value' to update the window by
   removing all elements that fall out of the new window
   which ends with 'current_interval_value'. And then return
   the max in the new value.

   @return the max element
   */
  const T& max_in_window(NumericValue current_interval_value)
  {
    update_window(current_interval_value);
    return value_array_->get(value_array_->find_max());
  }

  bool empty()
  {
    return value_array_->empty();
  }

  /**
   Returning the max element in its internal current window.

   @return the max element
   */
  const T& max_in_current_window()
  {
    return value_array_->get(value_array_->find_max());
  }

private:
  circular_buffer<T>* value_array_;
  circular_buffer<NumericValue>* interval_array_;

  NumericValue window_interval_;
  // how often to insert value to the window
  NumericValue update_rate_;
  T max_val_since_last_update_;
  //when do an insertion and update the window only when
  //(current_interval - last_interval_start) > update_rate
  NumericValue last_interval_start_;

  max_sliding_window(const max_sliding_window& b);
  const max_sliding_window& operator=(const max_sliding_window& b);

  /**
   Remove the head of the sliding window
   */
  void remove_front()
  {
    interval_array_->remove_front();
    value_array_->remove_front();
    return;
  }

  /**
   A new window is ended at 'current_interval_value', clear all
   elements that fall out of the window.
   */
  void update_window(NumericValue current_interval_value)
  {
    while(!empty())
    {
      NumericValue head_interval = interval_array_->front();
      //need to remove head
      if ((current_interval_value - head_interval) > window_interval_)
        remove_front();
      else
        break;
    }
    return;
  }
};

#endif
