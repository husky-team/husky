#include "core/mailbox.hpp"

#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "base/serialization.hpp"

namespace husky {
namespace {

using base::BinStream;

class TestMailbox : public testing::Test {
   public:
    TestMailbox() {}
    ~TestMailbox() {}

   protected:
    void SetUp() {}
    void TearDown() {}
};

TEST_F(TestMailbox, InitAndDelete) {
    zmq::context_t zmq_context;

    // Setup
    auto* el = new MailboxEventLoop(&zmq_context);
    el->set_process_id(0);
    auto* recver = new CentralRecver(&zmq_context, "inproc://test");
    auto* mailbox = new LocalMailbox(&zmq_context);
    mailbox->set_thread_id(0);
    el->register_mailbox(*mailbox);

    delete el;
    delete recver;
    delete mailbox;
}

TEST_F(TestMailbox, SendRecvOnce) {
    zmq::context_t zmq_context;

    // Setup
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // send a message
    BinStream send_bin_stream;
    int send_int = 1;
    send_bin_stream << send_int;
    mailbox.send(0, 0, 0, send_bin_stream);
    mailbox.send_complete(0, 0);

    // Recv the message
    mailbox.poll(0, 0);
    BinStream recv_bin_stream = mailbox.recv(0, 0);
    assert(mailbox.poll(0, 0) == false);
    int recv_int;
    recv_bin_stream >> recv_int;
    assert(recv_int == 1);
}

TEST_F(TestMailbox, SendRecvOnceAsync) {
    zmq::context_t zmq_context;

    // Setup
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // send a message
    BinStream send_bin_stream;
    int send_int = 1;
    send_bin_stream << send_int;
    mailbox.send(0, 0, 0, send_bin_stream);
    mailbox.send_complete(0, 0);

    // Recv the message
    while(1) {
        if (mailbox.poll_non_block(0, 0)) {
            BinStream recv_bin_stream = mailbox.recv(0, 0);
            assert(mailbox.poll_non_block(0, 0) == false);
            int recv_int;
            recv_bin_stream >> recv_int;
            assert(recv_int == 1);
            break;
        }
    }
}

TEST_F(TestMailbox, SendRecvOnceAsyncTimeout) {
    zmq::context_t zmq_context;

    // Setup
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // send a message
    BinStream send_bin_stream;
    int send_int = 1;
    send_bin_stream << send_int;
    mailbox.send(0, 0, 0, send_bin_stream);
    mailbox.send_complete(0, 0);

    // Recv the message
    bool if_recv = false;
    while(1) {
        bool flag = mailbox.poll_with_timeout(0, 0, 0.1);
        if(flag) {
            BinStream recv_bin_stream = mailbox.recv(0, 0);
            int recv_int;
            recv_bin_stream >> recv_int;
            assert(recv_int == 1);
            if_recv = true;
        } else if (if_recv) {
            break;
        }
    }
}

TEST_F(TestMailbox, SendRecvMultiple) {
    zmq::context_t zmq_context;

    // Setup
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox(&zmq_context);
    mailbox.set_thread_id(0);
    el.register_mailbox(mailbox);

    // send a message
    BinStream send_bin_stream;
    int send_int = 419;
    float send_float = 4.19;
    send_bin_stream << send_int;
    send_bin_stream << send_float;
    mailbox.send(0, 0, 0, send_bin_stream);
    mailbox.send_complete(0, 0);

    // Recv the message
    mailbox.poll(0, 0);
    BinStream recv_bin_stream = mailbox.recv(0, 0);
    assert(mailbox.poll(0, 0) == false);
    int recv_int;
    float recv_float;
    recv_bin_stream >> recv_int;
    recv_bin_stream >> recv_float;
    assert(recv_int == 419);
    assert(recv_float == static_cast<float>(4.19));
}

TEST_F(TestMailbox, Multithread) {
    zmq::context_t zmq_context;

    // Setup
    MailboxEventLoop el(&zmq_context);
    el.set_process_id(0);
    CentralRecver recver(&zmq_context, "inproc://test");
    LocalMailbox mailbox_0(&zmq_context);
    mailbox_0.set_thread_id(0);
    el.register_mailbox(mailbox_0);
    LocalMailbox mailbox_1(&zmq_context);
    mailbox_1.set_thread_id(1);
    el.register_mailbox(mailbox_1);

    // send a message
    BinStream send_bin_stream;
    int send_int = 419;
    float send_float = 4.19;
    send_bin_stream << send_int;
    send_bin_stream << send_float;
    mailbox_1.send(0, 0, 0, send_bin_stream);

    mailbox_1.send_complete(0, 0);
    mailbox_0.send_complete(0, 0);

    // Recv the message
    assert(mailbox_1.poll(0, 0) == false);

    mailbox_0.poll(0, 0);
    BinStream recv_bin_stream = mailbox_0.recv(0, 0);
    assert(mailbox_0.poll(0, 0) == false);
    int recv_int;
    float recv_float;
    recv_bin_stream >> recv_int;
    recv_bin_stream >> recv_float;
    assert(recv_int == 419);
    assert(recv_float == static_cast<float>(4.19));
}

TEST_F(TestMailbox, TwoProcesses) {
    // Setup thread 0 on process 0
    zmq::context_t zmq_context_0;
    MailboxEventLoop el_0(&zmq_context_0);
    el_0.set_process_id(0);
    CentralRecver recver_0(&zmq_context_0, "ipc://test-0");
    LocalMailbox mailbox_0(&zmq_context_0);
    mailbox_0.set_thread_id(0);
    el_0.register_mailbox(mailbox_0);

    // Setup thread 1 on process 1
    zmq::context_t zmq_context_1;
    MailboxEventLoop el_1(&zmq_context_1);
    el_1.set_process_id(1);
    CentralRecver recver_1(&zmq_context_1, "ipc://test-1");
    LocalMailbox mailbox_1(&zmq_context_1);
    mailbox_1.set_thread_id(1);
    el_1.register_mailbox(mailbox_1);

    // Connect
    el_0.register_peer_recver(1, "ipc://test-1");
    el_0.register_peer_thread(1, 1);
    el_1.register_peer_recver(0, "ipc://test-0");
    el_1.register_peer_thread(0, 0);

    // Send the message
    BinStream send_bin_stream;
    int send_int = 419;
    float send_float = 4.19;
    send_bin_stream << send_int;
    send_bin_stream << send_float;
    mailbox_0.send(1, 0, 0, send_bin_stream);

    mailbox_0.send_complete(0, 0);
    mailbox_1.send_complete(0, 0);

    // Recv the message
    assert(mailbox_0.poll(0, 0) == false);
    mailbox_1.poll(0, 0);
    BinStream recv_bin_stream = mailbox_1.recv(0, 0);
    assert(mailbox_1.poll(0, 0) == false);
    int recv_int;
    float recv_float;
    recv_bin_stream >> recv_int;
    recv_bin_stream >> recv_float;
    assert(recv_int == 419);
    assert(recv_float == static_cast<float>(4.19));
}

TEST_F(TestMailbox, Iterative) {
    // Setup thread 0 on process 0
    zmq::context_t zmq_context_0;
    MailboxEventLoop el_0(&zmq_context_0);
    el_0.set_process_id(0);
    CentralRecver recver_0(&zmq_context_0, "ipc://test-0");
    LocalMailbox mailbox_0(&zmq_context_0);
    mailbox_0.set_thread_id(0);
    el_0.register_mailbox(mailbox_0);

    // Setup thread 1 on process 1
    zmq::context_t zmq_context_1;
    MailboxEventLoop el_1(&zmq_context_1);
    el_1.set_process_id(1);
    CentralRecver recver_1(&zmq_context_1, "ipc://test-1");
    LocalMailbox mailbox_1(&zmq_context_1);
    mailbox_1.set_thread_id(1);
    el_1.register_mailbox(mailbox_1);

    // Connect
    el_0.register_peer_recver(1, "ipc://test-1");
    el_0.register_peer_thread(1, 1);
    el_1.register_peer_recver(0, "ipc://test-0");
    el_1.register_peer_thread(0, 0);

    for (int i = 0; i < 100; i++) {
        // Send the message
        BinStream send_bin_stream;
        int send_int = 419;
        float send_float = 4.19;
        send_bin_stream << send_int;
        send_bin_stream << send_float;
        mailbox_0.send(1, 0, i, send_bin_stream);

        mailbox_0.send_complete(0, i);
        mailbox_1.send_complete(0, i);

        // Recv the message
        assert(mailbox_0.poll(0, i) == false);
        mailbox_1.poll(0, i);
        BinStream recv_bin_stream = mailbox_1.recv(0, i);
        assert(mailbox_1.poll(0, i) == false);
        int recv_int;
        float recv_float;
        recv_bin_stream >> recv_int;
        recv_bin_stream >> recv_float;
        assert(recv_int == send_int);
        assert(recv_float == send_float);
    }
}

TEST_F(TestMailbox, OutOfOrder) {
    zmq::context_t zmq_context;
    auto* el = new MailboxEventLoop(&zmq_context);
    el->set_process_id(0);
    auto* recver = new CentralRecver(&zmq_context, "inproc://test");

    std::vector<LocalMailbox*> mailboxes;
    for (int i = 0; i < 4; i++) {
        mailboxes.push_back(new LocalMailbox(&zmq_context));
        mailboxes[i]->set_thread_id(i);
        el->register_mailbox(*mailboxes[i]);
    }

    std::vector<std::thread*> threads;
    for (int i = 0; i < 4; i++) {
        threads.push_back(new std::thread([=, &mailboxes]() {
            for (int j = 0; j < 100; j++) {
                for (int i_ = 0; i_ < 4; i_++) {
                    BinStream send_bin_stream;
                    send_bin_stream << i;
                    mailboxes[i]->send(i_, i, j, send_bin_stream);
                }
                for (int i_ = 0; i_ < 4; i_++)
                    mailboxes[i]->send_complete(i_, j);
                int sum = 0;
                BinStream recv_bin_stream;
                std::vector<std::pair<int, int>> poll_list = {{0, j}, {1, j}, {2, j}, {3, j}};
                int active_idx;
                while (mailboxes[i]->poll(poll_list, &active_idx)) {
                    recv_bin_stream = mailboxes[i]->recv(poll_list[active_idx].first, poll_list[active_idx].second);
                    int i_;
                    recv_bin_stream >> i_;
                    sum += i_;
                }
                ASSERT_EQ(sum, 6);
            }
        }));
    }

    for (int i = 0; i < 4; i++)
        threads[i]->join();
    delete recver;
    delete el;
    for (int i = 0; i < threads.size(); i++)
        delete mailboxes[i];
}

}  // namespace
}  // namespace husky
