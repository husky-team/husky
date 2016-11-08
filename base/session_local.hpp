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

#include <functional>
#include <utility>
#include <vector>

namespace husky {
namespace base {

// SessionLocalPriority class, the higher the value, the higher the priority
enum class SessionLocalPriority { Level1, Level2 };

class SessionLocal {
   public:
    static std::vector<std::function<void()>>& get_initializers();
    static std::vector<std::function<void()>>& get_finalizers();
    static std::vector<std::pair<SessionLocalPriority, std::function<void()>>>& get_thread_finalizers();
    static void register_initializer(std::function<void()> init);
    static void register_finalizer(std::function<void()> fina);
    static void register_thread_finalizer(SessionLocalPriority prior, std::function<void()> fina);
    static void initialize();
    static void finalize();
    static void thread_finalize();
    static bool is_session_end();

   private:
    static bool session_end_;
};

class RegSessionInitializer {
   public:
    explicit RegSessionInitializer(std::function<void()> init);
};

class RegSessionFinalizer {
   public:
    explicit RegSessionFinalizer(std::function<void()> fina);
};

class RegSessionThreadFinalizer {
   public:
    explicit RegSessionThreadFinalizer(SessionLocalPriority prior, std::function<void()> fina);
};

}  // namespace base
}  // namespace husky
