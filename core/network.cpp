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
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/param.h>
#include <unistd.h>
#endif
#ifdef _WIN32
#include <Winsock2.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include "core/utils.hpp"

namespace husky {

std::string get_hostname() {
    char hostname[1024];
    memset(hostname, 0, sizeof(hostname));

#ifdef __linux__
    gethostname(hostname, 1024);
#endif

#ifdef _WIN32
    gethostname(hostname, 1024);
#endif

    return std::string(hostname);
}

std::string ns_lookup(const std::string& name) {
    hostent* record = gethostbyname(name.c_str());
    ASSERT_MSG(record != NULL, (name + " cannot be resolved").c_str());
    in_addr* address = reinterpret_cast<in_addr*>(record->h_addr);
    std::string ip_address = inet_ntoa(*address);

    return ip_address;
}

std::set<std::string> get_self_ips() {
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

    freeifaddrs(ifap);
#endif

#ifdef _WIN32
    struct hostent* phe = gethostbyname(get_hostname().c_str());

    for (int i = 0; phe->h_addr_list[i] != 0; ++i) {
        struct in_addr addr;
        memcpy(&addr, phe->h_addr_list[i], sizeof(struct in_addr));
        ips.insert(inet_ntoa(addr));
    }
#endif
    return ips;
}

bool is_local(const std::string& name) {
    auto ip = ns_lookup(name);
    auto self_ips = get_self_ips();
    return self_ips.find(ip) != self_ips.end();
}

}  // namespace husky
