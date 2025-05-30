// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/http_client.h"

#include <absl/strings/str_split.h>
#include <fcntl.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/algorithm/string/predicate.hpp>
#include <filesystem>

#include "gtest/gtest_pred_impl.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "runtime/exec_env.h"
#include "service/backend_service.h"
#include "service/http_service.h"
#include "testutil/http_utils.h"
#include "util/md5.h"

namespace doris {

class HttpClientTestSimpleGetHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "test1") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        req->add_output_header(HttpHeaders::CONTENT_TYPE, "text/plain; version=0.0.4");
        bool is_acquire_md5 = !req->param("acquire_md5").empty();
        if (req->method() == HttpMethod::HEAD) {
            req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(5).c_str());
            if (is_acquire_md5) {
                Md5Digest md5;
                md5.update("md5sum", 6);
                md5.digest();
                req->add_output_header(HttpHeaders::CONTENT_MD5, md5.hex().c_str());
            }
            HttpChannel::send_reply(req);
        } else {
            std::string response = "test1";
            HttpChannel::send_reply(req, response);
        }
    }
};

class HttpClientTestSimplePostHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string user;
        std::string passwd;
        if (!parse_basic_auth(*req, &user, &passwd) || user != "test1") {
            HttpChannel::send_basic_challenge(req, "abc");
            return;
        }
        if (req->method() == HttpMethod::POST) {
            std::string post_body = req->get_request_body();
            if (!post_body.empty()) {
                HttpChannel::send_reply(req, post_body);
            } else {
                HttpChannel::send_reply(req, "empty");
            }
        }
    }
};

class HttpNotFoundHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "file not exist.");
    }
};

class HttpDownloadFileHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        do_file_response("/proc/self/exe", req, nullptr, true);
    }
};

class HttpBatchDownloadFileHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        if (req->param("check") == "true") {
            HttpChannel::send_reply(req, "OK");
        } else if (req->param("list") == "true") {
            do_dir_response(req->param("dir"), req, true);
        } else {
            std::vector<std::string> acquire_files =
                    absl::StrSplit(req->get_request_body(), "\n", absl::SkipWhitespace());
            HttpChannel::send_files(req, req->param("dir"), acquire_files);
        }
    }
};

static EvHttpServer* s_server = nullptr;
static int real_port = 0;
static std::string hostname = "";
static std::string address = "";
constexpr std::string_view TMP_DIR = "./http_test_tmp";

static HttpClientTestSimpleGetHandler s_simple_get_handler;
static HttpClientTestSimplePostHandler s_simple_post_handler;
static HttpNotFoundHandler s_not_found_handler;
static HttpDownloadFileHandler s_download_file_handler;
static HttpBatchDownloadFileHandler s_batch_download_file_handler;

class HttpClientTest : public testing::Test {
public:
    HttpClientTest() {}
    ~HttpClientTest() override {}

    static void SetUpTestCase() {
        s_server = new EvHttpServer(0);
        s_server->register_handler(GET, "/simple_get", &s_simple_get_handler);
        s_server->register_handler(HEAD, "/simple_get", &s_simple_get_handler);
        s_server->register_handler(POST, "/simple_post", &s_simple_post_handler);
        s_server->register_handler(GET, "/not_found", &s_not_found_handler);
        s_server->register_handler(HEAD, "/download_file", &s_download_file_handler);
        s_server->register_handler(HEAD, "/api/_tablet/_batch_download",
                                   &s_batch_download_file_handler);
        s_server->register_handler(GET, "/api/_tablet/_batch_download",
                                   &s_batch_download_file_handler);
        s_server->register_handler(POST, "/api/_tablet/_batch_download",
                                   &s_batch_download_file_handler);
        static_cast<void>(s_server->start());
        real_port = s_server->get_real_port();
        EXPECT_NE(0, real_port);
        address = "127.0.0.1:" + std::to_string(real_port);
        hostname = "http://" + address;
    }

    static void TearDownTestCase() { delete s_server; }
};

TEST_F(HttpClientTest, get_normal) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_method(GET);
    client.set_basic_auth("test1", "");
    std::string response;
    st = client.execute(&response);
    EXPECT_TRUE(st.ok()) << st;
    EXPECT_STREQ("test1", response.c_str());

    // for head
    st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_method(HEAD);
    client.set_basic_auth("test1", "");
    st = client.execute();
    EXPECT_TRUE(st.ok());
    uint64_t len = 0;
    st = client.get_content_length(&len);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(5, len);
}

TEST_F(HttpClientTest, download) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_basic_auth("test1", "");
    std::string local_file = ".http_client_test.dat";
    st = client.download(local_file);
    EXPECT_TRUE(st.ok());
    char buf[50];
    auto fp = fopen(local_file.c_str(), "r");
    auto size = fread(buf, 1, 50, fp);
    buf[size] = 0;
    EXPECT_STREQ("test1", buf);
    unlink(local_file.c_str());
}

TEST_F(HttpClientTest, get_failed) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_get");
    EXPECT_TRUE(st.ok());
    client.set_method(GET);
    std::string response;
    st = client.execute(&response);
    EXPECT_FALSE(st.ok());
}

TEST_F(HttpClientTest, post_normal) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_post");
    EXPECT_TRUE(st.ok());
    client.set_method(POST);
    client.set_basic_auth("test1", "");
    std::string response;
    std::string request_body = "simple post body query";
    st = client.execute_post_request(request_body, &response);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(response.length(), request_body.length());
    EXPECT_STREQ(response.c_str(), request_body.c_str());
}

TEST_F(HttpClientTest, post_failed) {
    HttpClient client;
    auto st = client.init(hostname + "/simple_pos");
    EXPECT_TRUE(st.ok());
    client.set_method(POST);
    client.set_basic_auth("test1", "");
    std::string response;
    std::string request_body = "simple post body query";
    st = client.execute_post_request(request_body, &response);
    EXPECT_FALSE(st.ok());
    std::string not_found = "404";
    EXPECT_TRUE(boost::algorithm::contains(st.to_string(), not_found));
}

TEST_F(HttpClientTest, not_found) {
    HttpClient client;
    std::string url = hostname + "/not_found";
    constexpr uint64_t kMaxTimeoutMs = 1000;

    auto get_cb = [&url](HttpClient* client) {
        std::string resp;
        RETURN_IF_ERROR(client->init(url));
        client->set_timeout_ms(kMaxTimeoutMs);
        return client->execute(&resp);
    };

    auto status = HttpClient::execute_with_retry(3, 1, get_cb);
    // libcurl is configured by CURLOPT_FAILONERROR
    EXPECT_FALSE(status.ok());
}

TEST_F(HttpClientTest, header_content_md5) {
    std::string url = hostname + "/simple_get";

    {
        // without md5
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(HEAD);
        client.set_basic_auth("test1", "");
        st = client.execute();
        EXPECT_TRUE(st.ok());
        uint64_t len = 0;
        st = client.get_content_length(&len);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(5, len);
        std::string md5;
        st = client.get_content_md5(&md5);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(md5.empty());
    }

    {
        // with md5
        HttpClient client;
        auto st = client.init(url + "?acquire_md5=true");
        EXPECT_TRUE(st.ok());
        client.set_method(HEAD);
        client.set_basic_auth("test1", "");
        st = client.execute();
        EXPECT_TRUE(st.ok());
        uint64_t len = 0;
        st = client.get_content_length(&len);
        EXPECT_TRUE(st.ok());
        EXPECT_EQ(5, len);
        std::string md5_value;
        st = client.get_content_md5(&md5_value);
        EXPECT_TRUE(st.ok());

        Md5Digest md5;
        md5.update("md5sum", 6);
        md5.digest();
        EXPECT_EQ(md5_value, md5.hex());
    }
}

TEST_F(HttpClientTest, download_file_md5) {
    std::string url = hostname + "/download_file";
    HttpClient client;
    auto st = client.init(url);
    EXPECT_TRUE(st.ok());
    client.set_method(HEAD);
    client.set_basic_auth("test1", "");
    st = client.execute();
    EXPECT_TRUE(st.ok());

    std::string md5_value;
    st = client.get_content_md5(&md5_value);
    EXPECT_TRUE(st.ok());

    int fd = open("/proc/self/exe", O_RDONLY);
    ASSERT_TRUE(fd >= 0);
    struct stat stat;
    ASSERT_TRUE(fstat(fd, &stat) >= 0);

    int64_t file_size = stat.st_size;
    Md5Digest md5;
    void* buf = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    md5.update(buf, file_size);
    md5.digest();
    munmap(buf, file_size);

    EXPECT_EQ(md5_value, md5.hex());
    close(fd);
}

TEST_F(HttpClientTest, escape_url) {
    HttpClient client;
    client._curl = curl_easy_init();
    auto check_result = [&client](const auto& input_url, const auto& output_url) -> bool {
        std::string escaped_url;
        if (!client._escape_url(input_url, &escaped_url).ok()) {
            return false;
        }
        if (escaped_url != output_url) {
            return false;
        }
        return true;
    };
    std::string input_A = hostname + "/download_file?token=oxof&file_name=02x_0.dat";
    std::string output_A = hostname + "/download_file?token=oxof&file_name=02x_0.dat";
    ASSERT_TRUE(check_result(input_A, output_A));

    std::string input_B = hostname + "/download_file?";
    std::string output_B = hostname + "/download_file?";
    ASSERT_TRUE(check_result(input_B, output_B));

    std::string input_C = hostname + "/download_file";
    std::string output_C = hostname + "/download_file";
    ASSERT_TRUE(check_result(input_C, output_C));

    std::string input_D = hostname + "/download_file?&";
    std::string output_D = hostname + "/download_file?&";
    ASSERT_TRUE(check_result(input_D, output_D));

    std::string input_E = hostname + "/download_file?key=0x2E";
    std::string output_E = hostname + "/download_file?key=0x2E";
    ASSERT_TRUE(check_result(input_E, output_E));

    std::string input_F = hostname + "/download_file?key=0x2E&key=%";
    std::string output_F = hostname + "/download_file?key=0x2E&key=%25";
    ASSERT_TRUE(check_result(input_F, output_F));

    std::string input_G = hostname + "/download_file?key=0x2E&key=%2E#section";
    std::string output_G = hostname + "/download_file?key=0x2E&key=%252E#section";
    ASSERT_TRUE(check_result(input_G, output_G));
}

TEST_F(HttpClientTest, enable_http_auth) {
    std::string origin_hostname = hostname;
    Defer defer {[&origin_hostname]() {
        hostname = origin_hostname;
        config::enable_all_http_auth = false;
    }};

    hostname = doris::global_test_http_host;

    {
        config::enable_all_http_auth = false;
        std::string url = hostname + "/api/health";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok());
        std::cout << "response = " << response << "\n";
        EXPECT_TRUE(response.find("To Be Added") != std::string::npos);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/api/health";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        std::string response;
        st = client.execute(&response);
        std::cout << "st = " << st << "\n";
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(!st.ok()) << st;
        EXPECT_TRUE(st.msg().find("The requested URL returned error") != std::string::npos);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/api/health";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_basic_auth("root", "");
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok());
        std::cout << "response = " << response << "\n";
        EXPECT_TRUE(response.find("To Be Added") != std::string::npos);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/api/health";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_basic_auth("root", "errorpasswd");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(!st.ok());
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(st.msg().find("403") != std::string::npos);
    }

    {
        config::enable_all_http_auth = false;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok());
        std::cout << "response = " << response << "\n";
        EXPECT_TRUE(response.size() != 0);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        std::string response;
        st = client.execute(&response);
        std::cout << "st = " << st << "\n";
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(!st.ok());
        EXPECT_TRUE(st.msg().find("The requested URL returned error") != std::string::npos);
    }
    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_basic_auth("root", "");
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok());
        std::cout << "response = " << response << "\n";
        EXPECT_TRUE(response.size() != 0);
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_basic_auth("root", "errorpasswd");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(!st.ok());
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(st.msg().find("403") != std::string::npos);
    }

    // valid token
    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_auth_token("valid_token");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(st.ok()) << st;
    }

    // invalid token
    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/metrics";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(GET);
        client.set_auth_token("invalid_token");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute(&response);
        EXPECT_TRUE(!st.ok()) << st;
    }

    {
        config::enable_all_http_auth = true;
        std::string url = hostname + "/api/glog/adjust";
        HttpClient client;
        auto st = client.init(url);
        EXPECT_TRUE(st.ok());
        client.set_method(POST);
        client.set_basic_auth("rootss", "");
        client.set_timeout_ms(200);
        std::string response;
        st = client.execute_post_request("level=1&module=xxx", &response);
        EXPECT_TRUE(!st.ok());
        std::cout << "response = " << response << "\n";
        std::cout << "st.msg() = " << st.msg() << "\n";
        EXPECT_TRUE(st.msg().find("403") != std::string::npos);
    }

    std::vector<std::string> check_get_list = {"/api/clear_cache/aa",
                                               "/api/running_pipeline_tasks",
                                               "/api/running_pipeline_tasks/223",
                                               "/api/query_pipeline_tasks/78902309190709864",
                                               "/api/be_process_thread_num",
                                               "/api/load_streams",
                                               "/api/show_config",
                                               "/api/shrink_mem"};

    for (auto endpoint : check_get_list) {
        std::cout << "endpint = " << endpoint << "\n";

        {
            config::enable_all_http_auth = true;
            std::string url = hostname + endpoint;
            HttpClient client;
            auto st = client.init(url);
            EXPECT_TRUE(st.ok());
            client.set_method(GET);
            std::string response;
            st = client.execute(&response);
            std::cout << "st = " << st << "\n";
            std::cout << "response = " << response << "\n";
            std::cout << "st.msg() = " << st.msg() << "\n";
            EXPECT_TRUE(!st.ok());
            EXPECT_TRUE(st.msg().find("The requested URL returned error") != std::string::npos);
        }

        {
            config::enable_all_http_auth = true;
            std::string url = hostname + endpoint;
            HttpClient client;
            auto st = client.init(url);
            EXPECT_TRUE(st.ok());
            client.set_method(GET);
            client.set_basic_auth("roxot", "errorpasswd");
            client.set_timeout_ms(200);
            std::string response;
            st = client.execute(&response);
            EXPECT_TRUE(!st.ok());
            std::cout << "response = " << response << "\n";
            std::cout << "st.msg() = " << st.msg() << "\n";
            EXPECT_TRUE(st.msg().find("403") != std::string::npos);
        }
    }
}

TEST_F(HttpClientTest, batch_download) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(TMP_DIR).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(TMP_DIR).ok());

    std::string root_dir(TMP_DIR);
    std::string remote_related_dir = root_dir + "/source";
    std::string local_dir = root_dir + "/target";
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(remote_related_dir).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(local_dir).ok());

    std::string remote_dir;
    EXPECT_TRUE(io::global_local_filesystem()->canonicalize(remote_related_dir, &remote_dir).ok());

    // 0. create dir source and prepare a large file exceeds 1MB
    {
        std::string large_file = remote_dir + "/a_large_file";
        int fd = open(large_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        ASSERT_TRUE(fd >= 0);
        std::string buf = "0123456789";
        for (int i = 0; i < 10; i++) {
            buf += buf;
        }
        for (int i = 0; i < 1024; i++) {
            ASSERT_TRUE(write(fd, buf.c_str(), buf.size()) > 0);
        }
        close(fd);

        // create some small files.
        for (int i = 0; i < 32; i++) {
            std::string small_file = remote_dir + "/small_file_" + std::to_string(i);
            fd = open(small_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
            ASSERT_TRUE(fd >= 0);
            ASSERT_TRUE(write(fd, buf.c_str(), buf.size()) > 0);
            close(fd);
        }

        // create a empty file
        std::string empty_file = remote_dir + "/empty_file";
        fd = open(empty_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        ASSERT_TRUE(fd >= 0);
        close(fd);

        empty_file = remote_dir + "/zzzz";
        fd = open(empty_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
        ASSERT_TRUE(fd >= 0);
        close(fd);
    }

    // 1. check remote support batch download
    Status st = is_support_batch_download(address);
    EXPECT_TRUE(st.ok());

    // 2. list remote files
    std::vector<std::pair<std::string, size_t>> file_info_list;
    st = list_remote_files_v2(address, "token", remote_dir, &file_info_list);
    EXPECT_TRUE(st.ok());

    // 3. download files
    if (file_info_list.size() > 64) {
        file_info_list.resize(64);
    }

    // sort file info list by file name
    std::sort(file_info_list.begin(), file_info_list.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    st = download_files_v2(address, "token", remote_dir, local_dir, file_info_list);
    EXPECT_TRUE(st.ok());
}

} // namespace doris
