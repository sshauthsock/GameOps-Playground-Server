#ifndef WEBSOCKET_SERVER_SSL_H
#define WEBSOCKET_SERVER_SSL_H

// SSL/TLS 지원 WebSocket 서버 (선택적 사용)
// 현재는 일반 WebSocket 서버 사용 중
// WSS가 필요한 경우 이 파일을 사용할 수 있음

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <memory>
#include <map>
#include <vector>
#include <thread>
#include <string>

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;

// SSL WebSocket 세션 클래스
class WebSocketSessionSSL : public std::enable_shared_from_this<WebSocketSessionSSL>
{
    websocket::stream<beast::ssl_stream<tcp::socket>> ws_;
    beast::flat_buffer buffer_;
    int client_fd_;
    std::function<void(int, const std::vector<char>&)> message_handler_;

public:
    explicit WebSocketSessionSSL(beast::ssl_stream<tcp::socket> stream, int client_fd,
                                std::function<void(int, const std::vector<char>&)> handler);

    void run();
    void send_message(const std::vector<char>& data);

private:
    void on_handshake(beast::error_code ec);
    void on_accept(beast::error_code ec);
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes_transferred);
    void on_write(beast::error_code ec, std::size_t bytes_transferred);
    void fail(beast::error_code ec, char const* what);
};

// SSL WebSocket 서버 클래스
class WebSocketServerSSL
{
    net::io_context ioc_;
    ssl::context ctx_;
    tcp::acceptor acceptor_;
    std::map<int, std::shared_ptr<WebSocketSessionSSL>> sessions_;
    std::function<void(int, const std::vector<char>&)> message_handler_;
    int next_client_fd_;
    std::thread server_thread_;
    std::string cert_file_;
    std::string key_file_;

public:
    WebSocketServerSSL(unsigned short port,
                      std::function<void(int, const std::vector<char>&)> handler,
                      const std::string& cert_file = "",
                      const std::string& key_file = "");
    ~WebSocketServerSSL();

    void start();
    void stop();
    void send_to_client(int client_fd, const std::vector<char>& data);
    void close_client(int client_fd);

private:
    void do_accept();
    void run();
    void load_certificates();
};

#endif

