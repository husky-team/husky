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

#ifdef _WIN32
#error "The networking support for Windows is not ready yet."
#endif

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/param.h>

#include <cstring>
#include <set>
#include <string>

#ifdef __linux__
#include <unistd.h>
#endif
#ifdef _WIN32
#include <Winsock2.h>
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
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
    gethostname(hostname, 1024);
    WSACleanup();
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
    return ips;
}

bool is_local(const std::string& name) {
    auto ip = ns_lookup(name);
    auto self_ips = get_self_ips();
    return self_ips.find(ip) != self_ips.end();
}

}  // namespace husky
