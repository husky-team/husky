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

#include "core/network.hpp"

#include <cstring>
#include <set>
#include <string>

#ifdef __linux__
#include <ifaddrs.h>
#endif

#include "boost/asio.hpp"

#include "core/utils.hpp"

namespace husky {

std::string get_hostname() { return boost::asio::ip::host_name(); }

std::set<std::string> get_ips(const std::string& name) {
    std::set<std::string> ips;

    boost::asio::io_service io_service;
    boost::asio::ip::tcp::resolver resolver(io_service);
    boost::asio::ip::tcp::resolver::query query(name, "");
    boost::asio::ip::tcp::resolver::iterator iter = resolver.resolve(query);
    boost::asio::ip::tcp::resolver::iterator end;
    while (iter != end) {
        ips.insert(iter->endpoint().address().to_string());
        iter++;
    }

    return ips;
}

std::set<std::string> get_local_ips() {
    std::set<std::string> ips;

#ifdef __linux__
    struct ifaddrs* ifap;
    getifaddrs(&ifap);

    char* addr;
    struct ifaddrs* ifa;
    struct sockaddr_in* sa;
    for (ifa = ifap; ifa; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr->sa_family == AF_INET) {
            sa = (struct sockaddr_in*) ifa->ifa_addr;
            addr = inet_ntoa(sa->sin_addr);
            ips.insert(addr);
        }
    }
#endif

#ifdef _WIN32
    struct hostent* phe = gethostbyname(get_hostname().c_str());
    for (int i = 0; phe->h_addr_list[i] != 0; i++) {
        struct in_addr addr;
        memcpy(&addr, phe->h_addr_list[i], sizeof(struct inaddr));
        ips.insert(inet_ntoa(addr));
    }
#endif

    return ips;
}

template <typename T>
bool has_overlap(const std::set<T>& s1, const std::set<T>& s2) {
    for (auto& x : s1)
        if (s2.find(x) != s2.end())
            return true;
    return false;
}

bool is_local(const std::string& name) { return has_overlap(get_local_ips(), get_ips(name)); }

}  // namespace husky
