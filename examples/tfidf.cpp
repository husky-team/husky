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

// Parameters
//
// mongo_server
// type: string
// info: Represent the server of mongo db
//
// mongo_db
// type: string
// info: Represent the database on mongo db
//
// mongo_collection
// type: string
// info: Represent the collection name
//
// mongo_user
// type: string
// info: Represent the user name for mongo db
//
// mongo_pwd
// type: string
// info: Represent the password for mongo db
//
// doc_id
// type: string
// info: Represent the key of a document and should be unique to every document
//
// doc_content
// type: string
// info: Represent the content of a document
//
// Configure example:
//
// mongo_server:172.16.104.62:20001
// mongo_db:hdb
// mongo_collection:enwiki
// mongo_user:username
// mongo_pwd:password
// doc_id:title
// doc_content:content

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "boost/tokenizer.hpp"
#include "boost/utility/string_ref.hpp"
#include "mongo/bson/bson.h"
#include "mongo/client/dbclient.h"

#include "base/serialization.hpp"
#include "core/engine.hpp"
#include "io/input/mongodb_inputformat.hpp"
#include "lib/aggregator_factory.hpp"

class Document {
   public:
    using KeyT = std::string;
    Document() = default;
    explicit Document(const KeyT& t) : title(t) {}
    KeyT title;
    std::vector<double> tf;
    std::vector<double> tf_idf;
    std::vector<std::string> words;
    int total_words = 0;
    const KeyT& id() const { return title; }

    friend husky::BinStream& operator<<(husky::BinStream& stream, const Document& doc) {
        stream << doc.title << doc.tf << doc.tf_idf << doc.words << doc.total_words;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Document& doc) {
        stream >> doc.title >> doc.tf >> doc.tf_idf >> doc.words >> doc.total_words;
        return stream;
    }
};

class Term {
   public:
    using KeyT = std::string;
    Term() = default;
    explicit Term(const KeyT& term) : termid(term) {}
    KeyT termid;
    double idf;
    const KeyT& id() const { return termid; }
};

void tfidf() {
    auto& document_list = husky::ObjListFactory::create_objlist<Document>();
    auto parse = [&](std::string& chunk) {
        mongo::BSONObj o = mongo::fromjson(chunk);
        Document doc(o.getStringField(husky::Context::get_param("doc_id")));
        std::string content = o.getStringField(husky::Context::get_param("doc_content"));
        doc.words.resize(0);
        std::vector<int> count;
        if (content.size() > 0) {
            boost::char_separator<char> sep(" \t\n.,()\'\":;!?<>");
            boost::tokenizer<boost::char_separator<char>> tok(content, sep);
            for (auto& w : tok) {
                doc.words.push_back(w);
                std::transform(doc.words.back().begin(), doc.words.back().end(), doc.words.back().begin(), ::tolower);
            }
            doc.total_words = doc.words.size();
            std::sort(doc.words.begin(), doc.words.end());
            int n = 0;
            for (int i = 0, j = 0; i < doc.words.size(); i = j) {
                for (j = i + 1; j < doc.words.size(); j++) {
                    if (doc.words.at(i).compare(doc.words.at(j)) != 0)
                        break;
                }
                count.push_back(j - i);
                doc.words[n++] = doc.words[i];
            }
            doc.words.resize(n);
            doc.words.shrink_to_fit();
            doc.tf.resize(doc.words.size());
            doc.tf_idf.resize(doc.words.size());
            for (int i = 0; i < doc.words.size(); i++) {
                doc.tf.at(i) = static_cast<double>(count.at(i)) / doc.total_words;
            }
        }
        document_list.add_object(doc);
    };
    husky::io::MongoDBInputFormat inputformat;
    inputformat.set_server(husky::Context::get_param("mongo_server"));
    inputformat.set_ns(husky::Context::get_param("mongo_db"), husky::Context::get_param("mongo_collection"));
    inputformat.set_auth(husky::Context::get_param("mongo_user"), husky::Context::get_param("mongo_pwd"));
    load(inputformat, parse);

    auto& term_list = husky::ObjListFactory::create_objlist<Term>();
    husky::lib::Aggregator<int> num_total_doc;
    auto& ac = husky::lib::AggregatorFactory::get_channel();
    auto& num_term =
        husky::ChannelFactory::create_push_combined_channel<int, husky::SumCombiner<int>>(document_list, term_list);
    auto& location_term =
        husky::ChannelFactory::create_push_channel<std::pair<std::string, int>>(document_list, term_list);

    husky::globalize(document_list);
    list_execute(document_list, {}, {&num_term, &location_term, &ac}, [&](Document& doc) {
        for (int i = 0; i < doc.words.size(); i++) {
            num_term.push(1, doc.words.at(i));
            location_term.push(std::make_pair(doc.id(), i), doc.words.at(i));
        }
        num_total_doc.update(1);
    });
    auto& location_term_and_idf =
        husky::ChannelFactory::create_push_channel<std::pair<int, float>>(term_list, document_list);
    list_execute(term_list, {&num_term}, {},
                 [&](Term& t) { t.idf = log(static_cast<double>(num_total_doc.get_value()) / num_term.get(t)); });

    list_execute(term_list, {&location_term}, {&location_term_and_idf}, [&](Term& t) {
        auto& msgs = location_term.get(t);
        for (int i = 0; i < msgs.size(); i++) {
            std::pair<int, float> push_msg = std::make_pair(int(msgs[i].second), t.idf);
            location_term_and_idf.push(push_msg, msgs[i].first);
        }
    });

    list_execute(document_list, {&location_term_and_idf}, {}, [&](Document& doc) {
        auto& msgs = location_term_and_idf.get(doc);

        for (int j = 0; j < msgs.size(); j++) {
            int i = msgs[j].first;
            doc.tf_idf.at(i) = doc.tf.at(i) * msgs[j].second;
        }
    });
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("mongo_server");
    args.push_back("mongo_db");
    args.push_back("mongo_collection");
    args.push_back("mongo_user");
    args.push_back("mongo_pwd");
    args.push_back("doc_id");
    args.push_back("doc_content");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(tfidf);
        return 0;
    }
    return 1;
}
