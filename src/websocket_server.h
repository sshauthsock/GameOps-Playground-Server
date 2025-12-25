#ifndef WEBSOCKET_SERVER_H
#define WEBSOCKET_SERVER_H

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <memory>
#include <map>
#include <vector>
#include <thread>

namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// WebSocket 세션 클래스
class WebSocketSession : public std::enable_shared_from_this<WebSocketSession>
{
    websocket::stream<tcp::socket> ws_;
    beast::flat_buffer buffer_;
    int client_fd_;  // 기존 TCP 서버와 호환을 위한 FD
    std::function<void(int, const std::vector<char>&)> message_handler_;
    std::function<void(int)> close_session_callback_;  // 세션 제거 콜백
    
    // Write 큐 및 상태 관리
    std::vector<std::vector<char>> write_queue_;
    bool is_writing_;

public:
    explicit WebSocketSession(tcp::socket socket, int client_fd,
                             std::function<void(int, const std::vector<char>&)> handler,
                             std::function<void(int)> close_callback);

    void run();
    void send_message(const std::vector<char>& data);

private:
    void on_accept(beast::error_code ec);
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes_transferred);
    void on_write(beast::error_code ec, std::size_t bytes_transferred);
    void do_write();
    void fail(beast::error_code ec, char const* what);
};

// WebSocket 서버 클래스
class WebSocketServer
{
    net::io_context ioc_;
    net::strand<net::io_context::executor_type> strand_;  // 스레드 안전성을 위한 strand
    tcp::acceptor acceptor_;
    std::map<int, std::shared_ptr<WebSocketSession>> sessions_;
    std::function<void(int, const std::vector<char>&)> message_handler_;
    std::function<void(int)> cleanup_callback_;  // 플레이어 정리 콜백
    int next_client_fd_;
    std::thread server_thread_;

public:
    WebSocketServer(unsigned short port,
                   std::function<void(int, const std::vector<char>&)> handler,
                   std::function<void(int)> cleanup_handler = nullptr);
    ~WebSocketServer();

    void start();
    void stop();
    void send_to_client(int client_fd, const std::vector<char>& data);
    void close_client(int client_fd);

private:
    void do_accept();
    void run();
    void do_send_to_client(int client_fd, const std::vector<char>& data);
};

#endif

