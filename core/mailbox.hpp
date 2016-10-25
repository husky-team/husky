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

#pragma once

#include <condition_variable>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zmq.hpp"

#include "base/concurrent_channel_store.hpp"
#include "base/concurrent_queue.hpp"
#include "base/hash.hpp"
#include "base/serialization.hpp"
#include "core/hash_ring.hpp"

namespace husky {

using base::ConcurrentChannelStore;
using base::ConcurrentQueue;
using base::BinStream;

class EventLoopConnector;
class MailboxEventLoop;

// Channels will come to this guy and get their BinStream
class LocalMailbox {
   public:
    explicit LocalMailbox(zmq::context_t* zmq_context);
    virtual ~LocalMailbox();

    inline int get_thread_id() const { return thread_id_; }
    void set_thread_id(int thread_id);
    void set_process_id(int process_id);

    /// \brief Use thie before receiving incoming incoming communication.
    ///
    /// This method will block when the incoming communication is not available.
    /// It will return true when there's available incoming communication.
    /// Return false when all incoming communication is received.
    ///
    /// @param channel_id ID of the Channel in interest.
    /// @param progress Progress of the Channel in interest.
    /// @return True when there's available incoming communication. Otherwise
    ///         it returns false.
    bool poll(int channel_id, int progress);

    /// \brief Poll from multiple Channel-Progress pairs.
    ///
    /// Similar as the poll(int channel_id, int progress) method. However, It
    /// supports multiple Channel-Progress pairs.
    ///
    /// @param[in] channel_progress_pairs Channel-Progress pairs to query.
    /// @param[out] active_idx The index of the pair that has incoming communication.
    /// @return True when there's available incoming communication. Otherwise
    ///         it returns false.
    bool poll(const std::vector<std::pair<int, int>>& channel_progress_pairs, int* active_idx);

    /// \brief Poll without blocking.
    ///
    /// Similar as the poll(int channel_id, int progress) method. However, it
    /// directly returns false when incoming communication is not available,
    /// even if they are still in-flight.
    ///
    /// @param channel_id ID of the Channel in interest.
    /// @param progress Progress of the Channel in interest.
    /// @return True when there's non-blocking incoming communication available.
    ///         Otherwise it returns false.
    bool poll_non_block(int channel_id, int progress);

    /// \brief Poll without blocking.
    ///
    /// Similar as the poll_non_block(int channel_id, int progress) method.
    /// However, it will wait for some time, and give up when the given time
    /// period is reached.
    ///
    /// @param channel_id ID of the Channel in interest.
    /// @param progress Progress of the Channel in interest.
    /// @param progress Maximum time to wait for (seconds).
    /// @return True when there's non-blocking incoming communication available.
    ///         Otherwise it returns false.
    bool poll_with_timeout(int channel_id, int progress, double timeout);

    /// \brief Send outgoing communication to a specific thread.
    ///
    /// This method can be used multiple times to send multiple BinStreams. After
    /// that, the `send_complete` method should be used to indicate the end of this
    /// batch of communication.
    ///
    /// @param thread_id ID of the destination worker thread.
    /// @param channel_id ID of the Channel for the communication.
    /// @param progress Progress of the communication. Progress should always
    ///        be increasing.
    /// @param bin_stream The actual communication in the form of BinStream.
    void send(int thread_id, int channel_id, int progress, BinStream& bin_stream);

    /// \brief Indicate that a round of outgoing communication finishes.
    ///
    /// This method must be called after a round of outgoing communication.
    ///
    /// @param channel_id Channel of the communication.
    /// @param progress Progress of the corresponding Channel.
    /// @param src_hash_ring Group of threads that issue the outgoing communication
    /// @param src_hash_ring Group of threads that will receive the communication
    void send_complete(int channel_id, int progress, HashRing* src_hash_ring, HashRing* dst_hash_ring);

    /// \brief Indicate that a round of outgoing communication finishes.
    ///
    /// Similar as send_complete(int channel_id, int progress, HashRing* src_hash_ring, HashRing* dst_hash_ring)
    /// except that the communication happen within the same group of machines.
    ///
    /// @param channel_id Channel of the communication.
    /// @param progress Progress of the corresponding Channel.
    /// @param src_hash_ring Group of threads that involes in the communication
    void send_complete(int channel_id, int progress, HashRing* hash_ring);

    /// \brief Receive incoming communication
    ///
    /// Receive incoming communication in the form of BinStream. It must be called
    /// after `poll`.
    ///
    /// @param channel_id ID of the Channel for the communication.
    /// @param progress Progress of the communication. Progress should always
    ///        be increasing.
    /// @return The actual incoming communication in the form of BinStream.
    BinStream recv(int channel_id, int progress);

    friend class MailboxEventLoop;

   protected:
    int thread_id_;
    int process_id_ = 0;
    zmq::context_t* zmq_context_;
    std::condition_variable poll_cv_;
    std::mutex notify_lock;

    ConcurrentChannelStore<ConcurrentQueue<BinStream*>> in_queue_;
    ConcurrentChannelStore<bool> comm_completed_;
    ConcurrentChannelStore<std::mutex> poll_mutex_;
    EventLoopConnector* event_loop_connector_;
};

class CentralRecver {
   public:
    // Create the recver thread and recver socket
    CentralRecver(zmq::context_t* zmq_context, const std::string& bind_addr);
    // Join the thread and free resources
    virtual ~CentralRecver();

   protected:
    void serve();

    zmq::context_t* zmq_context_;
    zmq::socket_t comm_recver_;
    std::thread* recver_thread_;
    std::string bind_addr_;
    EventLoopConnector* event_loop_connector_;
};

class MailboxEventLoop {
   public:
    // Create the event loop thread and the send socket
    explicit MailboxEventLoop(zmq::context_t* zmq_context);
    // Join the thread and free resources
    virtual ~MailboxEventLoop();

    void register_mailbox(LocalMailbox& local_mailbox);
    void set_process_id(int process_id);
    void register_peer_recver(int process_id, const std::string& addr);
    void register_peer_thread(int process_id, int thread_id);
    void register_event_handler(int event_type, std::function<void()> handler);

   protected:
    void recv_comm_handler();
    void _recv_comm_handler(int thread_id, int channel_id, int progress, BinStream* recv_bin_stream_ptr);
    void send_comm_handler();
    void _send_comm_handler(int thread_id, int channel_id, int progress, BinStream* send_bin_stream_ptr);
    void send_comm_complete_handler();
    void _send_comm_complete_handler(int channel_id, int progress, int num_local_threads,
                                     const std::vector<int>& global_pids);
    void recv_comm_complete_handler();
    void _recv_comm_complete_handler(int channel_id, int progress);
    void _recv_comm_complete_handler(int channel_id, int progress, int num_global_sync_processes);

    void serve();

    zmq::context_t* zmq_context_;
    zmq::socket_t event_recver_;
    std::unordered_map<int, LocalMailbox*> registered_mailbox_;
    std::unordered_map<int, zmq::socket_t*> sender_;
    std::unordered_map<int, int> tid_to_pid_;
    std::unordered_map<int, std::function<void()>> event_handler_;
    std::unordered_map<std::pair<int, int>, int> send_comm_complete_counter_;
    std::unordered_map<std::pair<int, int>, int> recv_comm_complete_counter_;
    std::thread* event_loop_thread_;
    int num_local_threads_ = 0;
    int num_global_processes_ = 1;
    int process_id_ = -1;
};

class EventLoopConnector {
   public:
    explicit EventLoopConnector(zmq::context_t* zmq_context);

    void generate_in_comm_event(int thread_id, int channel_id, int progress, BinStream* bin_stream);
    void generate_in_comm_complete_event(int channel_id, int progress, int num_global_sync_proceses);
    void generate_out_comm_event(int thread_id, int channel_id, int progress, BinStream& bin_stream);
    void generate_out_comm_complete_event(int channel_id, int progress, int num_local_sender_threads,
                                          std::vector<int>* global_pids_ptr);

   protected:
    zmq::socket_t event_sender_;
};

}  // namespace husky
