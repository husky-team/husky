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

#include "core/balance.hpp"

#include <numeric>  // for std::accumulate
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/context.hpp"

namespace husky {

std::unordered_map<int, int> base_balance_algo(std::unordered_map<int, int>& num_objs, int id) {
    auto map = base_balance_algo_for_all(num_objs);
    if (map.find(id) == map.end()) {
        map[id] = std::unordered_map<int, int>();
    }
    return map[id];
}

std::unordered_map<int, std::unordered_map<int, int>> base_balance_algo_for_all(
    std::unordered_map<int, int>& num_objs) {
    int average = (int) std::accumulate(
        num_objs.begin(), num_objs.end(), 0.0,
        [&](double a, std::pair<int, int> pair) -> double { return a + (double) pair.second / num_objs.size(); });

    auto greater = [](const std::pair<int, int>& left, const std::pair<int, int>& right) {
        return left.second < right.second;
    };

    auto less = [](const std::pair<int, int>& left, const std::pair<int, int>& right) {
        return left.second > right.second;
    };

    std::priority_queue<std::pair<int, int>, std::vector<std::pair<int, int>>, decltype(greater)> max_heap(greater);
    std::priority_queue<std::pair<int, int>, std::vector<std::pair<int, int>>, decltype(less)> min_heap(less);

    // the minimum number of objects a thread should have
    int at_least = average;
    // the maximum number of objects a thread should have
    int at_most = average + 1;

    for (auto& v : num_objs) {
        if (v.second > at_least) {
            max_heap.push(v);
        }
        if (v.second < at_most) {
            min_heap.push(v);
        }
    }

    // the migration plan
    // key: source global_tid
    // value: destination global_tid and number of objects transfered
    std::unordered_map<int, std::unordered_map<int, int>> res;
    while (!min_heap.empty() && !max_heap.empty()) {
        std::pair<int, int> min_pair = min_heap.top();
        min_heap.pop();
        const int dst_tid = min_pair.first;
        const int min_obj_num = min_pair.second;
        int already_moved = 0;
        while (!max_heap.empty() && min_obj_num + already_moved < at_least) {
            int num_to_be_moved = 0;
            std::pair<int, int> max_pair = max_heap.top();
            max_heap.pop();
            const int src_tid = max_pair.first;
            const int max_obj_num = max_pair.second;
            const int at_most_can_move = max_obj_num - at_least;
            const int need = at_least - (min_obj_num + already_moved);
            // check whether this thread has enough objects to be moved
            // if not
            if (need >= at_most_can_move) {
                already_moved += at_most_can_move;
                num_to_be_moved = at_most_can_move;
            } else {
                already_moved += need;
                num_to_be_moved = need;
                // if it still has excess objects can be moved
                // push back to the heap
                if ((max_obj_num - num_to_be_moved) >= at_most) {
                    max_heap.push(std::make_pair(src_tid, max_obj_num - num_to_be_moved));
                }
            }
            assert(num_to_be_moved >= 0);
            if (res.find(src_tid) == res.end()) {
                res[src_tid] = std::unordered_map<int, int>();
            }
            res[src_tid][dst_tid] = num_to_be_moved;
        }
    }

    return res;
}

std::unordered_map<int, int> local_balance_first_algo(std::unordered_map<int, int>& num_objs, int id) {
    auto map = local_balance_first_algo_for_all(num_objs);
    if (map.find(id) == map.end()) {
        map[id] = std::unordered_map<int, int>();
    }
    return map[id];
}

std::unordered_map<int, std::unordered_map<int, int>> local_balance_first_algo_for_all(
    std::unordered_map<int, int>& num_objs) {
    std::unordered_map<int, std::unordered_map<int, int>> res;
    std::unordered_map<int, std::unordered_map<int, int>> processes;

    int average = (int) std::accumulate(
        num_objs.begin(), num_objs.end(), 0.0,
        [&](double a, std::pair<int, int> pair) -> double { return a + (double) pair.second / num_objs.size(); });

    // group the information with respect to global_pid
    // so that we can first balance the objects of different threads in one process
    for (auto& p : num_objs) {
        int global_tid = p.first;
        int num_objs = p.second;
        int global_pid = husky::Context::get_process_id();
        if (processes.find(global_pid) != processes.end()) {
            processes[global_pid] = std::unordered_map<int, int>();
        }
        processes[global_pid][global_tid] = num_objs;
    }

    for (auto& p : processes) {
        int global_pid = p.first;
        std::unordered_map<int, std::unordered_map<int, int>> migrate_plans = base_balance_algo_for_all(p.second);
        for (auto& plan : migrate_plans) {
            int src_tid = plan.first;
            for (auto& move : plan.second) {
                int dst_tid = move.first;
                int num_to_move = move.second;
                if (res.find(src_tid) == res.end()) {
                    res[src_tid] = std::unordered_map<int, int>();
                }
                res[src_tid][dst_tid] = num_to_move;
                num_objs[src_tid] -= num_to_move;
                num_objs[dst_tid] += num_to_move;
            }
        }
    }

    std::unordered_map<int, std::unordered_map<int, int>> migrate_plans = base_balance_algo_for_all(num_objs);

    for (auto& plan : migrate_plans) {
        int src_tid = plan.first;
        for (auto& move : plan.second) {
            int dst_tid = move.first;
            int num_to_move = move.second;
            if (res.find(src_tid) == res.end()) {
                res[src_tid] = std::unordered_map<int, int>();
            }
            res[src_tid][dst_tid] += num_to_move;
        }
    }
    return res;
}

}  // namespace husky
