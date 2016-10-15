// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "core/mailbox.hpp"

#include <string>
#include <utility>
#include <vector>

#include "base/log.hpp"
#include "core/constants.hpp"
#include "core/network.hpp"
#include "core/utils.hpp"
#include "core/zmq_helpers.hpp"

namespace husky {

const char* kEventLoopListenAddress = "inproc://central-event-loop";

LocalMailbox::LocalMailbox(zmq::context_t* zmq_context) : zmq_context_(zmq_context) {
    event_loop_connector_ = new EventLoopConnector(zmq_context_);
    comm_completed_.init(false);
}

LocalMailbox::~LocalMailbox() { delete event_loop_connector_; }

void LocalMailbox::set_thread_id(int thread_id) { thread_id_ = thread_id; }

// TODO(fan) When messages are very fragmented, there may be
// a lot of locking, since each arrival of message will trigger
// a locking
bool LocalMailbox::poll(int channel_id, int progress) {
    // step 1: check the queue size
    if (in_queue_.get(channel_id, progress).size() > 0)
        return true;

    // step 2: check the flag
    // block when the queue is not empty but the flag is true
    // std::unique_lock<std::mutex> lock(poll_mutex_.get(channel_id, progress));
    std::unique_lock<std::mutex> lock(notify_lock);

    poll_cv_.wait(lock, [&]() {
        if (in_queue_.get(channel_id, progress).size() > 0)
            return true;
        if (comm_completed_.get(channel_id, progress))
            return true;

        return false;
    });

    if (in_queue_.get(channel_id, progress).size() > 0)
        return true;

    assert(comm_completed_.get(channel_id, progress));

    // The following is to re-use cells in comm_completed_.
    // It's based on the assumption that Channel progress
    // won't decrease
    if (progress - 1 >= 0)
        comm_completed_.get(channel_id, progress - 1) = false;

    return false;
}

// This function will not block the thread, it will return
// immediately whether there is message or not
bool LocalMailbox::poll_non_block(int channel_id, int progress) { return in_queue_.get(channel_id, progress).size(); }

// If there is message, this function will return True immediately.
// Otherwise, this function will wait until a message come or the
// waiting time specified by the user is used up
bool LocalMailbox::poll_with_timeout(int channel_id, int progress, double timeout) {
    // step 1: check the queue size
    if (in_queue_.get(channel_id, progress).size() > 0)
        return true;

    // step 2: check the flag
    // block when the queue is not empty but the flag is true
    std::unique_lock<std::mutex> lock(poll_mutex_.get(channel_id, progress));

    poll_cv_.wait_for(lock, std::chrono::duration<double>(timeout), [&]() {
        if (in_queue_.get(channel_id, progress).size() > 0)
            return true;
        if (comm_completed_.get(channel_id, progress))
            return true;

        return false;
    });

    if (in_queue_.get(channel_id, progress).size() > 0)
        return true;
    return false;
}

bool LocalMailbox::poll(const std::vector<std::pair<int, int>>& channel_progress_pairs, int* active_idx) {
    for (int i = 0; i < channel_progress_pairs.size(); i++) {
        if (in_queue_.get(channel_progress_pairs[i].first, channel_progress_pairs[i].second).size() > 0) {
            *active_idx = i;
            return true;
        }
    }

    std::unique_lock<std::mutex> lock(notify_lock);
    poll_cv_.wait(lock, [&]() {
        for (auto& chnl_prgs_pair : channel_progress_pairs)
            if (in_queue_.get(chnl_prgs_pair.first, chnl_prgs_pair.second).size() > 0)
                return true;

        int num_completes = 0;
        for (auto& chnl_prgs_pair : channel_progress_pairs)
            if (comm_completed_.get(chnl_prgs_pair.first, chnl_prgs_pair.second))
                num_completes += 1;
        if (num_completes == channel_progress_pairs.size())
            return true;

        return false;
    });

    for (int i = 0; i < channel_progress_pairs.size(); i++) {
        if (in_queue_.get(channel_progress_pairs[i].first, channel_progress_pairs[i].second).size() > 0) {
            *active_idx = i;
            return true;
        }
    }

    for (auto& chnl_prgs_pair : channel_progress_pairs)
        if (chnl_prgs_pair.second - 1 >= 0)
            comm_completed_.get(chnl_prgs_pair.first, chnl_prgs_pair.second - 1) = false;

    return false;
}

BinStream LocalMailbox::recv(int channel_id, int progress) {
    ASSERT_MSG(in_queue_.get(channel_id, progress).size() > 0, "Please poll before recv");

    BinStream* recv_bin_stream_ptr = in_queue_.get(channel_id, progress).pop();
    BinStream recv_bin_stream(std::move(*recv_bin_stream_ptr));
    return recv_bin_stream;
}

void LocalMailbox::send(int thread_id, int channel_id, int progress, BinStream& bin_stream) {
    event_loop_connector_->generate_out_comm_event(thread_id, channel_id, progress, bin_stream);
}

void LocalMailbox::send_complete(int channel_id, int progress) {
    event_loop_connector_->generate_out_comm_complete_event(channel_id, progress);
}

CentralRecver::CentralRecver(zmq::context_t* zmq_context, const std::string& bind_addr)
    : zmq_context_(zmq_context), comm_recver_(*zmq_context_, ZMQ_PULL) {
    bind_addr_ = bind_addr;
    comm_recver_.bind(bind_addr_);
    event_loop_connector_ = new EventLoopConnector(zmq_context_);
    recver_thread_ = new std::thread([&]() { serve(); });
}

CentralRecver::~CentralRecver() {
    zmq::socket_t destroyer(*zmq_context_, ZMQ_PUSH);
    std::string connect_addr = bind_addr_;
    if (bind_addr_.find("*") != -1) {
        int idx = bind_addr_.find("*");
        connect_addr = bind_addr_.substr(0, idx) + get_hostname() + bind_addr_.substr(idx + 1);
    }
    destroyer.connect(connect_addr);
    int destroy_recver_magic = -1;
    zmq_send_int32(&destroyer, destroy_recver_magic);
    recver_thread_->join();

    delete recver_thread_;
    delete event_loop_connector_;
}

void CentralRecver::serve() {
    while (true) {
        int thread_id = zmq_recv_int32(&comm_recver_);
        if (thread_id == -1)
            break;

        if (thread_id == -2) {
            int channel_id = zmq_recv_int32(&comm_recver_);
            int progress = zmq_recv_int32(&comm_recver_);
            event_loop_connector_->generate_in_comm_complete_event(channel_id, progress);
            continue;
        }

        int channel_id = zmq_recv_int32(&comm_recver_);
        int progress = zmq_recv_int32(&comm_recver_);
        BinStream* bin_stream_ptr = new BinStream();
        *bin_stream_ptr = zmq_recv_binstream(&comm_recver_);
        event_loop_connector_->generate_in_comm_event(thread_id, channel_id, progress, bin_stream_ptr);
    }
}

MailboxEventLoop::MailboxEventLoop(zmq::context_t* zmq_context)
    : zmq_context_(zmq_context), event_recver_(*zmq_context_, ZMQ_PULL) {
    event_recver_.bind(kEventLoopListenAddress);

    // register event handlers
    register_event_handler(MAILBOX_EVENT_SEND_COMM, std::bind(&MailboxEventLoop::send_comm_handler, this));
    register_event_handler(MAILBOX_EVENT_SEND_COMM_END, std::bind(&MailboxEventLoop::send_comm_complete_handler, this));
    register_event_handler(MAILBOX_EVENT_RECV_COMM, std::bind(&MailboxEventLoop::recv_comm_handler, this));
    register_event_handler(MAILBOX_EVENT_RECV_COMM_END, std::bind(&MailboxEventLoop::recv_comm_complete_handler, this));

    event_loop_thread_ = new std::thread([&]() { serve(); });
}

MailboxEventLoop::~MailboxEventLoop() {
    zmq::socket_t event_sender(*zmq_context_, ZMQ_PUSH);
    event_sender.connect(kEventLoopListenAddress);
    zmq_send_int32(&event_sender, MAILBOX_EVENT_DESTROY);

    event_loop_thread_->join();
    delete event_loop_thread_;
    for (auto& pair : sender_)
        delete pair.second;
}

void MailboxEventLoop::serve() {
    while (true) {
        int event_type = zmq_recv_int32(&event_recver_);
        if (event_type == MAILBOX_EVENT_DESTROY)
            break;
        ASSERT_MSG(event_handler_.find(event_type) != event_handler_.end(), "Unknown event type.");
        event_handler_[event_type]();
    }
}

void MailboxEventLoop::register_mailbox(LocalMailbox& local_mailbox) {
    int tid = local_mailbox.get_thread_id();
    ASSERT_MSG(registered_mailbox_.find(tid) == registered_mailbox_.end(),
               "Shouldn't register a same mailbox more than once");

    ASSERT_MSG(process_id_ != -1, "Need to set process id before registering mailbox");
    tid_to_pid_[tid] = process_id_;

    num_local_threads_ += 1;
    registered_mailbox_[tid] = &local_mailbox;
}

void MailboxEventLoop::set_process_id(int process_id) { process_id_ = process_id; }

void MailboxEventLoop::register_event_handler(int event_type, std::function<void()> handler) {
    event_handler_[event_type] = handler;
}

void MailboxEventLoop::recv_comm_handler() {
    int thread_id = zmq_recv_int32(&event_recver_);
    int channel_id = zmq_recv_int32(&event_recver_);
    int progress = zmq_recv_int32(&event_recver_);
    BinStream* recv_bin_stream_ptr = reinterpret_cast<BinStream*>(zmq_recv_int64(&event_recver_));
    _recv_comm_handler(thread_id, channel_id, progress, recv_bin_stream_ptr);
}

void MailboxEventLoop::_recv_comm_handler(int thread_id, int channel_id, int progress, BinStream* recv_bin_stream_ptr) {
    ASSERT_MSG(registered_mailbox_.find(thread_id) != registered_mailbox_.end(),
               ("[ERROR] Local mailbox for " + std::to_string(thread_id) + " does not exist").c_str());

    auto& mailbox = *(registered_mailbox_[thread_id]);

    mailbox.in_queue_.get(channel_id, progress).push(std::move(recv_bin_stream_ptr));
    mailbox.poll_cv_.notify_one();
}

void MailboxEventLoop::send_comm_handler() {
    int thread_id = zmq_recv_int32(&event_recver_);
    int channel_id = zmq_recv_int32(&event_recver_);
    int progress = zmq_recv_int32(&event_recver_);
    BinStream* bin_stream_ptr = reinterpret_cast<BinStream*>(zmq_recv_int64(&event_recver_));
    _send_comm_handler(thread_id, channel_id, progress, bin_stream_ptr);
}

void MailboxEventLoop::_send_comm_handler(int thread_id, int channel_id, int progress, BinStream* send_bin_stream_ptr) {
    int pid = tid_to_pid_[thread_id];
    if (pid != process_id_) {
        zmq_sendmore_int32(sender_[pid], thread_id);
        zmq_sendmore_int32(sender_[pid], channel_id);
        zmq_sendmore_int32(sender_[pid], progress);
        zmq_send_binstream(sender_[pid], *send_bin_stream_ptr);
        delete send_bin_stream_ptr;
    } else {
        // push it to the recv queue of the corresponding local mailbox
        _recv_comm_handler(thread_id, channel_id, progress, send_bin_stream_ptr);
    }
}

void MailboxEventLoop::send_comm_complete_handler() {
    int channel_id = zmq_recv_int32(&event_recver_);
    int progress = zmq_recv_int32(&event_recver_);
    _send_comm_complete_handler(channel_id, progress);
}

void MailboxEventLoop::_send_comm_complete_handler(int channel_id, int progress) {
    auto chnl_prgs_pair = std::make_pair(channel_id, progress);
    if (send_comm_complete_counter_.find(chnl_prgs_pair) == send_comm_complete_counter_.end())
        send_comm_complete_counter_[chnl_prgs_pair] = 0;

    send_comm_complete_counter_[chnl_prgs_pair] += 1;
    if (send_comm_complete_counter_[chnl_prgs_pair] == num_local_threads_) {
        for (auto& pid_sender_pair : sender_) {
            auto* send_sock = pid_sender_pair.second;
            int send_comm_complete_magic = -2;
            zmq_sendmore_int32(send_sock, send_comm_complete_magic);
            zmq_sendmore_int32(send_sock, channel_id);
            zmq_send_int32(send_sock, progress);
        }
        _recv_comm_complete_handler(channel_id, progress);
        send_comm_complete_counter_.erase(chnl_prgs_pair);
    }
}

void MailboxEventLoop::recv_comm_complete_handler() {
    int channel_id = zmq_recv_int32(&event_recver_);
    int progress = zmq_recv_int32(&event_recver_);
    _recv_comm_complete_handler(channel_id, progress);
}

void MailboxEventLoop::_recv_comm_complete_handler(int channel_id, int progress) {
    auto chnl_prgs_pair = std::make_pair(channel_id, progress);
    if (recv_comm_complete_counter_.find(chnl_prgs_pair) == recv_comm_complete_counter_.end())
        recv_comm_complete_counter_[chnl_prgs_pair] = 0;

    recv_comm_complete_counter_[chnl_prgs_pair] += 1;
    if (recv_comm_complete_counter_[chnl_prgs_pair] == num_global_processes_) {
        for (auto& tid_mailbox_pair : registered_mailbox_) {
            int thread_id = tid_mailbox_pair.first;
            auto& mailbox = *(registered_mailbox_[thread_id]);
            std::unique_lock<std::mutex> cv_lock(mailbox.notify_lock);
            mailbox.comm_completed_.get(channel_id, progress) = true;
            cv_lock.unlock();
            mailbox.poll_cv_.notify_one();
        }
        recv_comm_complete_counter_.erase(chnl_prgs_pair);
    }
}

void MailboxEventLoop::register_peer_recver(int process_id, const std::string& addr) {
    ASSERT_MSG(sender_.count(process_id) == 0, "Register the same peer recver more than once");
    sender_[process_id] = new zmq::socket_t(*zmq_context_, ZMQ_PUSH);
    int linger = 2000;
    sender_[process_id]->setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
    sender_[process_id]->connect(addr);
    num_global_processes_ += 1;
}

void MailboxEventLoop::register_peer_thread(int process_id, int thread_id) { tid_to_pid_[thread_id] = process_id; }

EventLoopConnector::EventLoopConnector(zmq::context_t* zmq_context) : event_sender_(*zmq_context, ZMQ_PUSH) {
    event_sender_.connect(kEventLoopListenAddress);
}

void EventLoopConnector::generate_in_comm_event(int thread_id, int channel_id, int progress,
                                                BinStream* bin_stream_ptr) {
    zmq_sendmore_int32(&event_sender_, MAILBOX_EVENT_RECV_COMM);
    zmq_sendmore_int32(&event_sender_, thread_id);
    zmq_sendmore_int32(&event_sender_, channel_id);
    zmq_sendmore_int32(&event_sender_, progress);
    zmq_send_int64(&event_sender_, reinterpret_cast<uint64_t>(bin_stream_ptr));
}

void EventLoopConnector::generate_in_comm_complete_event(int channel_id, int progress) {
    zmq_sendmore_int32(&event_sender_, MAILBOX_EVENT_RECV_COMM_END);
    zmq_sendmore_int32(&event_sender_, channel_id);
    zmq_send_int32(&event_sender_, progress);
}

void EventLoopConnector::generate_out_comm_event(int thread_id, int channel_id, int progress, BinStream& bin_stream) {
    zmq_sendmore_int32(&event_sender_, MAILBOX_EVENT_SEND_COMM);
    zmq_sendmore_int32(&event_sender_, thread_id);
    zmq_sendmore_int32(&event_sender_, channel_id);
    zmq_sendmore_int32(&event_sender_, progress);
    auto* bin_stream_ptr = new BinStream(std::move(bin_stream));
    zmq_send_int64(&event_sender_, reinterpret_cast<uint64_t>(bin_stream_ptr));
}

void EventLoopConnector::generate_out_comm_complete_event(int channel_id, int progress) {
    zmq_sendmore_int32(&event_sender_, MAILBOX_EVENT_SEND_COMM_END);
    zmq_sendmore_int32(&event_sender_, channel_id);
    zmq_send_int32(&event_sender_, progress);
}

}  // namespace husky
